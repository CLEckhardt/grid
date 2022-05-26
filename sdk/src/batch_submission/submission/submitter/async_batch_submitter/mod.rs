// Copyright 2022 Cargill Incorporated
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use async_trait::async_trait;

use std::fmt;

use crate::{
    batch_submission::{
        submission::{
            submitter::{RunnableSubmitter, RunningSubmitter},
            submitter_observer::SubmitterObserver,
            url_resolver::UrlResolver,
        },
        Submission,
    },
    error::{ClientError, InternalError},
    scope_id::ScopeId,
};

// Number of times a submitter task will retry submission in quick succession
const RETRY_ATTEMPTS: u16 = 10;
// Time the submitter will wait to repoll after receiving None, in milliseconds
const POLLING_INTERVAL: u64 = 1000;

// Implementing generics now

pub struct BatchSubmitterBuilder<
    S: 'static + ScopeId,
    Q: 'static + Iterator<Item = Submission<S>> + Send,
> {
    url_resolver: Option<&'static Box<dyn UrlResolver<Id = S>>>,
    queue: Option<Q>,
    observer: Option<&'static Box<dyn SubmitterObserver<Id = S> + Send>>,
    mock_execution: bool,
}

impl<S: 'static + ScopeId, Q: 'static + Iterator<Item = Submission<S>> + Send>
    BatchSubmitterBuilder<S, Q>
{
    fn new() -> Self {
        Self {
            url_resolver: None,
            queue: None,
            observer: None,
            mock_execution: false,
        }
    }

    fn with_url_resolver(&mut self, url_resolver: &'static Box<dyn UrlResolver<Id = S>>) {
        self.url_resolver = Some(url_resolver);
    }

    fn with_queue(&mut self, queue: Q) {
        self.queue = Some(queue);
    }

    fn with_observer(&mut self, observer: &'static Box<dyn SubmitterObserver<Id = S> + Send>) {
        self.observer = Some(observer);
    }

    fn with_mock_submission_command(&mut self) {
        self.mock_execution = true;
    }

    fn build(self) -> Result<BatchRunnableSubmitter<S, Q>, InternalError> {
        let queue = match self.queue {
            Some(q) => q,
            None => {
                return Err(InternalError::with_message(
                    "Cannot build BatchRunnableSubmitter, missing queue.".to_string(),
                ))
            }
        };
        let observer = match self.observer {
            Some(o) => o,
            None => {
                return Err(InternalError::with_message(
                    "Cannot build BatchRunnableSubmitter, missing observer.".to_string(),
                ))
            }
        };
        if self.mock_execution {
            let command_factory = match self.url_resolver {
                Some(u) => MockSubmissionCommandFactory::new(u),
                None => {
                    return Err(InternalError::with_message(
                        "Cannot build BatchRunnableSubmitter, missing url resolver.".to_string(),
                    ))
                }
            };
            BatchRunnableSubmitter::new(queue, observer, Box::new(command_factory))
        } else {
            let command_factory = match self.url_resolver {
                Some(u) => SubmissionCommandFactory::new(u),
                None => {
                    return Err(InternalError::with_message(
                        "Cannot build BatchRunnableSubmitter, missing url resolver.".to_string(),
                    ))
                }
            };
            BatchRunnableSubmitter::new(queue, observer, Box::new(command_factory))
        }
    }
}

pub struct BatchRunnableSubmitter<
    S: 'static + ScopeId,
    Q: 'static + Iterator<Item = Submission<S>> + Send,
> {
    queue: Q,
    observer: &'static Box<dyn SubmitterObserver<Id = S> + Send>,
    command_factory: Box<dyn ExecuteCommandFactory<S>>,
    leader_channel: (
        std::sync::mpsc::Sender<TerminateMessage>,
        std::sync::mpsc::Receiver<TerminateMessage>,
    ),
    listener_channel: (
        std::sync::mpsc::Sender<TerminateMessage>,
        std::sync::mpsc::Receiver<TerminateMessage>,
    ),
    submission_channel: (
        std::sync::mpsc::Sender<BatchMessage<S>>,
        std::sync::mpsc::Receiver<BatchMessage<S>>,
    ),
    spawner_channel: (
        tokio::sync::mpsc::Sender<CentralMessage<S>>,
        tokio::sync::mpsc::Receiver<CentralMessage<S>>,
    ),
    runtime: tokio::runtime::Runtime,
}

impl<S: 'static + ScopeId, Q: 'static + Iterator<Item = Submission<S>> + Send>
    BatchRunnableSubmitter<S, Q>
{
    fn new(
        queue: Q,
        observer: &'static Box<dyn SubmitterObserver<Id = S> + Send>,
        command_factory: Box<dyn ExecuteCommandFactory<S>>,
    ) -> Result<Self, InternalError> {
        Ok(Self {
            queue,
            observer,
            command_factory,
            leader_channel: std::sync::mpsc::channel(),
            listener_channel: std::sync::mpsc::channel(),
            submission_channel: std::sync::mpsc::channel(),
            spawner_channel: tokio::sync::mpsc::channel(64),
            runtime: tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_name("submitter_async_runtime")
                .build()
                .map_err(|e| InternalError::with_message(format!("{:?}", e)))?,
        })
    }
}

impl<S: 'static + ScopeId, Q: 'static + Iterator<Item = Submission<S>> + Send>
    RunnableSubmitter<S, Q> for BatchRunnableSubmitter<S, Q>
{
    type RunningSubmitter = BatchRunningSubmitter;

    fn run(self) -> Result<BatchRunningSubmitter, InternalError> {
        // Move subcomponents out of self
        let mut queue = self.queue;
        let observer = self.observer;
        let submitter_command_factory = self.command_factory;

        // Create channels for termination messages
        let (leader_tx, leader_rx) = self.leader_channel;
        let (listener_tx, listener_rx) = self.listener_channel;

        // Channel for messages from the async tasks to the sync listener thread
        let (tx_submission, rx_submission) = self.submission_channel;
        // Clone the sender so that the leader thread can notify the listener thread that it will
        // start submitting a batch
        let tx_leader = tx_submission.clone();

        // Channel for messges from the leader thread to the task-spawning thread
        let (tx_spawner, mut rx_spawner) = self.spawner_channel;

        let runtime = self.runtime;
        // Move the asnyc runtime to a separate thread so it doesn't block this one
        let runtime_handle = std::thread::Builder::new()
            .name("submitter_async_runtime_host".to_string())
            .spawn(move || {
                runtime.block_on(async move {
                    while let Some(msg) = rx_spawner.recv().await {
                        match msg {
                            CentralMessage::NewTask(t) => {
                                tokio::spawn(TaskHandler::spawn(
                                    t,
// PROBLEM HERE
// Can't clone because that makes the factory trait not object-safe
// Can't use reference because the factory isn't static
                                    submitter_command_factory.clone(),
                                ));
                            }
                            CentralMessage::Terminate => break,
                        }
                    }
                })
            })
            .map_err(|e| InternalError::with_message(format!("{:?}", e)))?;

        // Set up and run the leader thread
        let leader_handle = std::thread::Builder::new()
            .name("submitter_poller".to_string())
            .spawn(move || {
                loop {
                    // Check for shutdown command
                    match leader_rx.try_recv() {
                        Ok(_) => {
                            // Send terminate message to async runtime
                            if let Err(e) = tx_spawner.blocking_send(CentralMessage::Terminate) {
                                error!("Error sending terminate messsage to runtime: {:?}", e)
                            };
                            break;
                        }
                        Err(std::sync::mpsc::TryRecvError::Disconnected) => break,
                        Err(std::sync::mpsc::TryRecvError::Empty) => (),
                    }
                    // Poll for next batch and submit it
                    match queue.next() {
                        Some(b) => {
                            info!("Batch {}: received from queue", &b.batch_header());
                            if let Err(e) = tx_leader.send(BatchMessage::SubmissionNotification((
                                b.batch_header().clone(),
                                b.scope_id().clone(),
                            ))) {
                                error!("Error sending submission notification message: {:?}", e)
                            }
                            if let Err(e) = tx_spawner.blocking_send(CentralMessage::NewTask(
                                NewTask::new(tx_submission.clone(), b),
                            )) {
                                error!("Error sending NewTask message: {:?}", e)
                            };
                        }
                        None => {
                            std::thread::sleep(tokio::time::Duration::from_millis(POLLING_INTERVAL))
                        }
                    }
                }
            })
            .map_err(|e| InternalError::with_message(format!("{:?}", e)))?;

        // Set up and run the listener thread
        let listener_handle = std::thread::Builder::new()
            .name("submitter_listener".to_string())
            .spawn(move || {
                loop {
                    // Check for shutdown command
                    match listener_rx.try_recv() {
                        Ok(_) => break,
                        Err(std::sync::mpsc::TryRecvError::Disconnected) => break,
                        Err(std::sync::mpsc::TryRecvError::Empty) => (),
                    }
                    // Check for submisison response
                    if let Ok(msg) = rx_submission.try_recv() {
                        match msg {
                            BatchMessage::SubmissionNotification((batch_header, scope_id)) => {
                                // 0 signifies pre-submission
                                observer.notify(batch_header, scope_id, Some(0), None)
                            }
                            BatchMessage::SubmissionResponse(s) => {
                                info!(
                                    "Batch {id}: received submission response [{code}] after \
                                    {attempts} attempt(s)",
                                    id = &s.batch_header,
                                    code = &s.status,
                                    attempts = &s.attempts
                                );
                                observer.notify(
                                    s.batch_header,
                                    s.scope_id,
                                    Some(s.status),
                                    Some(s.message),
                                )
                            }
                            BatchMessage::ErrorResponse(e) => {
                                error!(
                                    "Submission error for batch {}: {:?}",
                                    &e.batch_header, &e.error
                                );
                                observer.notify(
                                    e.batch_header,
                                    e.scope_id,
                                    None,
                                    Some(e.error.to_string()),
                                );
                            }
                        }
                    }
                }
            })
            .map_err(|e| InternalError::with_message(format!("{:?}", e)))?;

        Ok(BatchRunningSubmitter {
            runtime_handle,
            leader_handle,
            leader_tx,
            listener_handle,
            listener_tx,
        })
    }
}

pub struct BatchRunningSubmitter {
    runtime_handle: std::thread::JoinHandle<()>,
    leader_handle: std::thread::JoinHandle<()>,
    leader_tx: std::sync::mpsc::Sender<TerminateMessage>,
    listener_handle: std::thread::JoinHandle<()>,
    listener_tx: std::sync::mpsc::Sender<TerminateMessage>,
}

impl RunningSubmitter for BatchRunningSubmitter {
    fn signal_shutdown(&self) -> Result<(), InternalError> {
        self.leader_tx
            .send(TerminateMessage::Terminate)
            .map_err(|e| {
                InternalError::with_message(format!(
                    "Error sending termination message to leader thread: {:?}",
                    e
                ))
            })?;
        self.listener_tx
            .send(TerminateMessage::Terminate)
            .map_err(|e| {
                InternalError::with_message(format!(
                    "Error sending termination message to listener thread: {:?}",
                    e
                ))
            })?;
        Ok(())
    }

    /// Wind down and stop the submission service.
    fn shutdown(self) -> Result<(), InternalError> {
        self.leader_handle
            .join()
            .map_err(|e| InternalError::with_message(format!("{:?}", e)))?;
        self.runtime_handle
            .join()
            .map_err(|e| InternalError::with_message(format!("{:?}", e)))?;
        self.listener_handle
            .join()
            .map_err(|e| InternalError::with_message(format!("{:?}", e)))?;
        Ok(())
    }
}

pub trait ExecuteCommandFactory<S: ScopeId>: Clone + Sync + Send {
    fn new_command(&self, submission: Submission<S>) -> Box<dyn ExecuteCommand<S>>;
}

#[derive(Clone)]
struct SubmissionCommandFactory<S: ScopeId> {
    url_resolver: &'static Box<dyn UrlResolver<Id = S>>,
}

impl<S: ScopeId> SubmissionCommandFactory<S> {
    fn new(url_resolver: &'static Box<dyn UrlResolver<Id = S>>) -> Self {
        Self { url_resolver }
    }
}

impl<S: ScopeId> ExecuteCommandFactory<S> for SubmissionCommandFactory<S> {
    fn new_command(&self, submission: Submission<S>) -> Box<dyn ExecuteCommand<S>> {
        Box::new(SubmissionCommand {
            url_resolver: self.url_resolver,
            submission,
            attempts: 0,
        })
    }
}

#[derive(Clone)]
struct MockSubmissionCommandFactory<S: ScopeId> {
    url_resolver: &'static Box<dyn UrlResolver<Id = S>>,
}

impl<S: ScopeId> MockSubmissionCommandFactory<S> {
    fn new(url_resolver: &'static Box<dyn UrlResolver<Id = S>>) -> Self {
        Self { url_resolver }
    }
}

impl<S: ScopeId> ExecuteCommandFactory<S> for MockSubmissionCommandFactory<S> {
    fn new_command(&self, submission: Submission<S>) -> Box<dyn ExecuteCommand<S>> {
        let _ = submission;
        Box::new(MockSubmissionCommand {
            url_resolver: self.url_resolver,
            attempts: 0,
        })
    }
}

struct MockSubmissionCommand<S: ScopeId> {
    url_resolver: &'static Box<dyn UrlResolver<Id = S>>,
    attempts: u16,
}

#[async_trait]
impl<S: ScopeId> ExecuteCommand<S> for MockSubmissionCommand<S> {
    async fn execute(&mut self) -> Result<SubmissionResponse<S>, reqwest::Error> {
        todo!()
    }
}

#[async_trait]
pub trait ExecuteCommand<S: ScopeId>: Sync + Send {
    async fn execute(&mut self) -> Result<SubmissionResponse<S>, reqwest::Error>;
}

//
//
//
//
//
//
#[derive(Debug)]
// Responsible for executing the submission request
struct SubmissionCommand<S: ScopeId> {
    url_resolver: &'static Box<dyn UrlResolver<Id = S>>,
    submission: Submission<S>,
    attempts: u16,
}

#[async_trait]
impl<S: ScopeId> ExecuteCommand<S> for SubmissionCommand<S> {
    async fn execute(&mut self) -> Result<SubmissionResponse<S>, reqwest::Error> {
        let client = reqwest::Client::builder()
            .timeout(tokio::time::Duration::from_secs(15))
            .build()?;

        self.attempts += 1;

        let res = client
            .post(&self.url_resolver.url(&self.submission.scope_id()))
            .body(self.submission.serialized_batch().clone())
            .send()
            .await?;

        Ok(SubmissionResponse::new(
            self.submission.batch_header().clone(),
            self.submission.scope_id().clone(),
            res.status().as_u16(),
            res.text().await?,
            self.attempts,
        ))
    }
}

#[derive(Debug, PartialEq)]
// Responsible for controlling retry behavior
struct SubmissionController;

impl SubmissionController {
    async fn run<S: ScopeId>(
        mut command: Box<dyn ExecuteCommand<S>>,
    ) -> Result<SubmissionResponse<S>, ClientError> {
        let mut wait: u64 = 250;
        let mut response: Result<SubmissionResponse<S>, reqwest::Error> = command.execute().await;
        for _ in 1..RETRY_ATTEMPTS {
            match &response {
                Ok(res) => match &res.status {
                    200 => break,
                    503 => {
                        tokio::time::sleep(tokio::time::Duration::from_millis(wait)).await;
                        response = command.execute().await;
                    }
                    _ => break,
                },
                Err(e) => {
                    if e.is_timeout() {
                        tokio::time::sleep(tokio::time::Duration::from_millis(wait)).await;
                        response = command.execute().await;
                    } else {
                        // This error is returned outside of the loop and not behind a &
                        break;
                    }
                }
            }
            wait += 500;
        }
        let res = response.map_err(ClientError::from)?;
        Ok(res)
    }
}

// Task unit within the async runtime
// Responsible for messaging with the synchronous listener thread
struct TaskHandler;

impl TaskHandler {
    async fn spawn<'a, S: ScopeId>(
        task: NewTask<S>,
        submission_command_factory: Box<dyn ExecuteCommandFactory<S>>,
    ) {
        let batch_header = task.submission.batch_header().clone();
        let scope_id = task.submission.scope_id().clone();
        let submission_command = submission_command_factory.new_command(task.submission);
        let submission: Result<SubmissionResponse<S>, ClientError> =
            SubmissionController::run(submission_command).await;

        let task_message = match submission {
            Ok(s) => BatchMessage::SubmissionResponse(s),
            Err(e) => BatchMessage::ErrorResponse(ErrorResponse {
                batch_header,
                scope_id,
                error: e.to_string(),
            }),
        };
        let _ = task.tx.send(task_message);
    }
}
//
//
//
//
//

#[derive(Debug, PartialEq)]
// Carries the submission response from the http client back through the submitter to the observer
struct SubmissionResponse<S: ScopeId> {
    batch_header: String,
    scope_id: S,
    status: u16,
    message: String,
    attempts: u16,
}

impl<S: ScopeId> SubmissionResponse<S> {
    fn new(batch_header: String, scope_id: S, status: u16, message: String, attempts: u16) -> Self {
        Self {
            batch_header,
            scope_id,
            status,
            message,
            attempts,
        }
    }
}

#[derive(Debug, PartialEq)]
// A message about a batch; sent between threads
enum BatchMessage<S: ScopeId> {
    SubmissionNotification((String, S)),
    SubmissionResponse(SubmissionResponse<S>),
    ErrorResponse(ErrorResponse<S>),
}

#[derive(Debug)]
// A message sent from the leader thread to the async runtime
enum CentralMessage<S: ScopeId> {
    NewTask(NewTask<S>),
    Terminate,
}

#[derive(Debug)]
// A message used to instruct the leader and listener threads to terminate
enum TerminateMessage {
    Terminate,
}

// The object required for an async task to function
// Provides the batch and a channel sender with which the task communicates back to the listener
// thread about the batch
struct NewTask<S: ScopeId> {
    tx: std::sync::mpsc::Sender<BatchMessage<S>>,
    submission: Submission<S>,
}

impl<S: ScopeId> NewTask<S> {
    fn new(tx: std::sync::mpsc::Sender<BatchMessage<S>>, submission: Submission<S>) -> Self {
        Self { tx, submission }
    }
}

impl<S: ScopeId> fmt::Debug for NewTask<S> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "{:?}", self.submission)
    }
}

#[derive(Debug, PartialEq)]
// Communicates an errror message from the task handler to the listener thread
struct ErrorResponse<S: ScopeId> {
    batch_header: String,
    scope_id: S,
    error: String,
}

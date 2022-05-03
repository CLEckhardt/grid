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

//! Manages the submission of the batches to the DLT. This implementation is async.
//!
//! For more information, see the
//! [design
//! doc](https://grid.hyperledger.org/community/planning/batch_submitter.html)

use std::{fmt, iter::Iterator, thread, time};

use log::{error, info};
use tokio::runtime::Builder;

use super::{
    batches::Submission, RunnableSubmitter, RunningSubmitter, ScopeId, SubmitterBuilder,
    SubmitterObserver, UrlResolver,
};
use crate::error::{ClientError, InternalError};

// Number of times a submitter task will retry submission in quick succession
const RETRY_ATTEMPTS: u16 = 10;
// Time the submitter will wait to repoll after receiving None, in milliseconds
const POLLING_INTERVAL: u64 = 1000;

//
// OBJECTS THAT MOVE DATA THROUGH THE SUBMITTER

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

//
// SUBMITTER SUBCOMPONENTS

#[derive(Debug, PartialEq)]
// Responsible for executing the submission request
struct SubmissionCommand<'a, S: ScopeId, R: 'a + UrlResolver<Id = S>> {
    url_resolver: &'a R,
    submission: Submission<S>,
    attempts: u16,
}

impl<'a, S: ScopeId, R: 'a + UrlResolver<Id = S>> SubmissionCommand<'a, S, R> {
    fn new(submission: Submission<S>, url_resolver: &'a R) -> Self {
        Self {
            url_resolver,
            submission,
            attempts: 0,
        }
    }

    async fn execute(&mut self) -> Result<SubmissionResponse<S>, reqwest::Error> {
        let client = reqwest::Client::builder()
            .timeout(time::Duration::from_secs(15))
            .build()?;

        self.attempts += 1;

        let res = client
            .post(self.url_resolver.url(&self.submission.scope_id()))
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
struct SubmissionController<'a, S: ScopeId, R: 'a + UrlResolver<Id = S>> {
    command: SubmissionCommand<'a, S, R>,
}

impl<'a, S: ScopeId, R: 'a + UrlResolver<Id = S>> SubmissionController<'a, S, R> {
    fn new(submission: Submission<S>, url_resolver: &'a R) -> Self {
        Self {
            command: SubmissionCommand::new(submission, url_resolver),
        }
    }

    async fn run(&mut self) -> Result<SubmissionResponse<S>, ClientError> {
        let mut wait: u64 = 250;
        let mut response: Result<SubmissionResponse<S>, reqwest::Error> =
            self.command.execute().await;
        for _ in 1..RETRY_ATTEMPTS {
            match &response {
                Ok(res) => match &res.status {
                    200 => break,
                    503 => {
                        tokio::time::sleep(time::Duration::from_millis(wait)).await;
                        response = self.command.execute().await;
                    }
                    _ => break,
                },
                Err(e) => {
                    if e.is_timeout() {
                        tokio::time::sleep(time::Duration::from_millis(wait)).await;
                        response = self.command.execute().await;
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
    async fn spawn<'a, S: ScopeId, R: 'a + UrlResolver<Id = S>>(
        task: NewTask<S>,
        url_resolver: &'a R,
    ) {
        let batch_header = task.submission.batch_header().clone();
        let scope_id = task.submission.scope_id().clone();
        let submission: Result<SubmissionResponse<S>, ClientError> =
            SubmissionController::new(task.submission, url_resolver)
                .run()
                .await;

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

pub struct BatchSubmitterBuilder<
    S: 'static + ScopeId,
    R: 'static + UrlResolver<Id = S> + Sync + Send,
    Q: 'static + Iterator<Item = Submission<S>> + Send,
    O: 'static + SubmitterObserver<Id = S> + Send,
> {
    url_resolver: Option<&'static R>,
    queue: Option<Q>,
    observer: Option<O>,
}

impl<
        S: ScopeId,
        R: UrlResolver<Id = S> + Sync + Send,
        Q: Iterator<Item = Submission<S>> + Send,
        O: SubmitterObserver<Id = S> + Send,
    > SubmitterBuilder<S, R, Q, O> for BatchSubmitterBuilder<S, R, Q, O>
{
    type RunnableSubmitter = BatchRunnableSubmitter<S, R, Q, O>;

    fn new() -> Self {
        Self {
            url_resolver: None,
            queue: None,
            observer: None,
        }
    }

    fn with_url_resolver(&mut self, url_resolver: &'static R) {
        self.url_resolver = Some(url_resolver);
    }

    fn with_queue(&mut self, queue: Q) {
        self.queue = Some(queue);
    }

    fn with_observer(&mut self, observer: O) {
        self.observer = Some(observer);
    }

    // Should maybe break out the error cases
    fn build(self) -> Result<Self::RunnableSubmitter, InternalError> {
        match (self.url_resolver, self.queue, self.observer) {
            (Some(r), Some(q), Some(o)) => Ok(BatchRunnableSubmitter {
                url_resolver: r,
                queue: q,
                observer: o,
                leader_channel: std::sync::mpsc::channel(),
                listener_channel: std::sync::mpsc::channel(),
                submission_channel: std::sync::mpsc::channel(),
                spawner_channel: tokio::sync::mpsc::channel(64),
                runtime: Builder::new_multi_thread()
                    .enable_all()
                    .thread_name("submitter_async_runtime")
                    .build()
                    .map_err(|e| InternalError::with_message(format!("{:?}", e)))?,
            }),
            _ => Err(InternalError::with_message(
                "Error building runnable submitter WSI: missing required field".to_string(),
            )),
        }
    }
}

pub struct BatchRunnableSubmitter<
    S: ScopeId,
    R: 'static + UrlResolver<Id = S> + Sync + Send,
    Q: Iterator<Item = Submission<S>> + Send,
    O: SubmitterObserver<Id = S> + Send,
> {
    url_resolver: &'static R,
    queue: Q,
    observer: O,
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

impl<
        S: 'static + ScopeId,
        R: 'static + UrlResolver<Id = S> + Sync + Send,
        Q: 'static + Iterator<Item = Submission<S>> + Send,
        O: 'static + SubmitterObserver<Id = S> + Send,
    > RunnableSubmitter<S, R, Q, O> for BatchRunnableSubmitter<S, R, Q, O>
{
    type RunningSubmitter = BatchRunningSubmitter;

    fn run(self) -> Result<Self::RunningSubmitter, InternalError> {
        // Move subcomponents out of self
        let mut queue = self.queue;
        let observer = self.observer;
        let url_resolver = self.url_resolver;

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
                                tokio::spawn(TaskHandler::spawn(t, url_resolver));
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
                        None => std::thread::sleep(time::Duration::from_millis(POLLING_INTERVAL)),
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
    runtime_handle: thread::JoinHandle<()>,
    leader_handle: thread::JoinHandle<()>,
    leader_tx: std::sync::mpsc::Sender<TerminateMessage>,
    listener_handle: thread::JoinHandle<()>,
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

/*
#[cfg(test)]
mod tests {

    use super::{
        super::{addresser::BatchAddresser, BatchTrackingId},
        *,
    };
    use crate::batch_tracking::store::{TrackingBatch, TrackingBatchBuilder};
    use crate::hex;
    use actix_web::{post, rt, App, HttpResponse, HttpServer, Responder};
    use cylinder::{secp256k1::Secp256k1Context, Context};
    use std::time::{SystemTime, UNIX_EPOCH};
    use transact::protocol::{
        batch::BatchBuilder,
        transaction::{HashMethod, TransactionBuilder},
    };

    // Implement a crude "random" Bernoulli distribution sampler for the /echo_maybe endpoint
    // Avoids importing the rand crate, and this doesn't need to be a good random sampler
    // Returns true roughly 1/3 of the time
    fn poorly_random() -> bool {
        let now = SystemTime::now();
        let time = now.duration_since(UNIX_EPOCH).unwrap().as_micros();
        (time % 3) == 0
    }

    // endpoints for rest APIs uses in tests
    #[post("/echo")]
    async fn echo(req_body: String) -> impl Responder {
        HttpResponse::Ok().body(req_body)
    }

    #[post("/echo_maybe")]
    async fn echo_maybe(req_body: String) -> impl Responder {
        let v = poorly_random();
        if v {
            println!("ok");
            HttpResponse::Ok().body(req_body)
        } else {
            println!("try again!");
            HttpResponse::ServiceUnavailable().finish()
        }
    }

    async fn run_rest_api(port: &str) -> std::io::Result<()> {
        HttpServer::new(|| App::new().service(echo).service(echo_maybe))
            .bind(format!("127.0.0.1:{}", port))?
            .run()
            .await
    }

    // We should make it easier to get a mock batch
    struct MockTrackingBatch;

    static FAMILY_NAME: &str = "test_family";
    static FAMILY_VERSION: &str = "0.1";
    static KEY1: &str = "111111111111111111111111111111111111111111111111111111111111111111";
    static KEY2: &str = "222222222222222222222222222222222222222222222222222222222222222222";
    static KEY3: &str = "333333333333333333333333333333333333333333333333333333333333333333";
    static KEY4: &str = "444444444444444444444444444444444444444444444444444444444444444444";
    static KEY5: &str = "555555555555555555555555555555555555555555555555555555555555555555";
    static KEY6: &str = "666666666666666666666666666666666666666666666666666666666666666666";
    static KEY7: &str = "777777777777777777777777777777777777777777777777777777777777777777";
    static NONCE: &str = "f9kdzz";
    static BYTES2: [u8; 4] = [0x05, 0x06, 0x07, 0x08];

    impl MockTrackingBatch {
        fn create(service_id: String) -> TrackingBatch {
            let context = Secp256k1Context::new();
            let key = context.new_random_private_key();
            let signer = context.new_signer(key);

            let pair = TransactionBuilder::new()
                .with_batcher_public_key(hex::parse_hex(KEY1).unwrap())
                .with_dependencies(vec![KEY2.to_string(), KEY3.to_string()])
                .with_family_name(FAMILY_NAME.to_string())
                .with_family_version(FAMILY_VERSION.to_string())
                .with_inputs(vec![
                    hex::parse_hex(KEY4).unwrap(),
                    hex::parse_hex(&KEY5[0..4]).unwrap(),
                ])
                .with_nonce(NONCE.to_string().into_bytes())
                .with_outputs(vec![
                    hex::parse_hex(KEY6).unwrap(),
                    hex::parse_hex(&KEY7[0..4]).unwrap(),
                ])
                .with_payload_hash_method(HashMethod::Sha512)
                .with_payload(BYTES2.to_vec())
                .build(&*signer)
                .unwrap();

            let batch_1 = BatchBuilder::new()
                .with_transactions(vec![pair])
                .build(&*signer)
                .unwrap();

            TrackingBatchBuilder::default()
                .with_batch(batch_1)
                .with_service_id(service_id)
                .with_signer_public_key(KEY1.to_string())
                .with_submitted(false)
                .with_created_at(111111)
                .build()
                .unwrap()
        }
    }

    #[test]
    fn test_batch_submitter_batch_envelope_create() {
        let test_addresser_wo_serv: Box<dyn Addresser + std::marker::Send> =
            Box::new(BatchAddresser::new("http://127.0.0.1:8080", None));
        let test_addresser_w_serv: Box<dyn Addresser + std::marker::Send> = Box::new(
            BatchAddresser::new("http://127.0.0.1:8080", Some("service_id")),
        );

        let mock_batch_1 = MockTrackingBatch::create("".to_string());
        let mock_batch_id_1 = BatchTrackingId::create(mock_batch_1);
        let mock_batch_2 = MockTrackingBatch::create("123-abc".to_string());
        let mock_batch_id_2 = BatchTrackingId::create(mock_batch_2);

        let test_batch_submission_wo_serv = BatchSubmission {
            id: mock_batch_id_1.clone(),
            service_id: None,
            serialized_batch: vec![1, 1, 1, 1],
        };
        let test_batch_submission_w_serv = BatchSubmission {
            id: mock_batch_id_2.clone(),
            service_id: Some("123-abc".to_string()),
            serialized_batch: vec![2, 2, 2, 2],
        };

        assert_eq!(
            BatchEnvelope::create(
                test_batch_submission_wo_serv.clone(),
                &test_addresser_wo_serv
            )
            .unwrap(),
            BatchEnvelope {
                id: mock_batch_id_1,
                address: "http://127.0.0.1:8080".to_string(),
                payload: vec![1, 1, 1, 1],
            }
        );
        assert_eq!(
            BatchEnvelope::create(test_batch_submission_w_serv.clone(), &test_addresser_w_serv)
                .unwrap(),
            BatchEnvelope {
                id: mock_batch_id_2,
                address: "http://127.0.0.1:8080?service_id=123-abc".to_string(),
                payload: vec![2, 2, 2, 2],
            }
        );
        assert!(
            BatchEnvelope::create(test_batch_submission_wo_serv, &test_addresser_w_serv).is_err()
        );
        assert!(
            BatchEnvelope::create(test_batch_submission_w_serv, &test_addresser_wo_serv).is_err()
        );
    }

    #[test]
    fn test_batch_submitter_submission_response_new() {
        let mock_batch = MockTrackingBatch::create("".to_string());
        let mock_batch_id = BatchTrackingId::create(mock_batch);

        let response = SubmissionResponse::new(
            mock_batch_id.clone(),
            200,
            "Everything is ok".to_string(),
            1,
        );

        assert_eq!(&response.id, &mock_batch_id);
        assert_eq!(&response.status, &200);
        assert_eq!(&response.message, &"Everything is ok".to_string());
        assert_eq!(&response.attempts, &1);
    }

    #[test]
    fn test_batch_submitter_submission_command_new() {
        let mock_batch = MockTrackingBatch::create("".to_string());
        let mock_batch_id = BatchTrackingId::create(mock_batch);
        let test_batch_envelope = BatchEnvelope {
            id: mock_batch_id.clone(),
            address: "http://127.0.0.1:8080".to_string(),
            payload: vec![1, 1, 1, 1],
        };
        let test_submission_command = SubmissionCommand::new(test_batch_envelope);
        assert_eq!(
            test_submission_command,
            SubmissionCommand {
                batch_envelope: BatchEnvelope {
                    id: mock_batch_id,
                    address: "http://127.0.0.1:8080".to_string(),
                    payload: vec![1, 1, 1, 1],
                },
                attempts: 0,
            }
        )
    }

    #[test]
    fn test_batch_submitter_submission_command_execute() {
        let mock_batch = MockTrackingBatch::create("".to_string());
        let mock_batch_id = BatchTrackingId::create(mock_batch);
        // Test batch with echo endpoint
        let test_batch_envelope = BatchEnvelope {
            id: mock_batch_id.clone(),
            address: "http://127.0.0.1:8085/echo".to_string(),
            payload: vec![1, 1, 1, 1],
        };
        let mut test_submission_command = SubmissionCommand::new(test_batch_envelope);

        let expected_response =
            SubmissionResponse::new(mock_batch_id, 200, "\u{1}\u{1}\u{1}\u{1}".to_string(), 1);

        // Start REST API
        std::thread::Builder::new()
            .name("test_rest_api_runtime".to_string())
            .spawn(move || {
                rt::System::new("rest_api").block_on(async { run_rest_api("8085").await.unwrap() })
            })
            .unwrap();

        let test_execute = Builder::new_current_thread()
            .thread_name("test_thread_submission_command")
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move { test_submission_command.execute().await.unwrap() });

        assert_eq!(test_execute, expected_response);
    }

    #[test]
    fn test_batch_submitter_submission_controller_new() {
        let mock_batch = MockTrackingBatch::create("".to_string());
        let mock_batch_id = BatchTrackingId::create(mock_batch);
        let test_batch_envelope = BatchEnvelope {
            id: mock_batch_id.clone(),
            address: "http://127.0.0.1:8080/echo".to_string(),
            payload: vec![1, 1, 1, 1],
        };
        let test_submission_controller = SubmissionController::new(test_batch_envelope);
        assert_eq!(
            test_submission_controller,
            SubmissionController {
                command: SubmissionCommand {
                    batch_envelope: BatchEnvelope {
                        id: mock_batch_id.clone(),
                        address: "http://127.0.0.1:8080/echo".to_string(),
                        payload: vec![1, 1, 1, 1],
                    },
                    attempts: 0,
                }
            }
        )
    }

    #[test]
    fn test_batch_submitter_submission_controller_run() {
        let mock_batch = MockTrackingBatch::create("".to_string());
        let mock_batch_id = BatchTrackingId::create(mock_batch);
        // Test batch with echo endpoint
        let test_batch_envelope_1 = BatchEnvelope {
            id: mock_batch_id.clone(),
            address: "http://127.0.0.1:8086/echo".to_string(),
            payload: vec![1, 1, 1, 1],
        };
        let mut test_submission_controller_1 = SubmissionController::new(test_batch_envelope_1);

        // Test batch with echo_maybe endpoint to test retry behavior
        let test_batch_envelope_2 = BatchEnvelope {
            id: mock_batch_id.clone(),
            address: "http://127.0.0.1:8086/echo_maybe".to_string(),
            payload: vec![1, 1, 1, 1],
        };
        let mut test_submission_controller_2 = SubmissionController::new(test_batch_envelope_2);

        let expected_response =
            SubmissionResponse::new(mock_batch_id, 200, "\u{1}\u{1}\u{1}\u{1}".to_string(), 1);

        // Start REST API
        std::thread::Builder::new()
            .name("submitter_async_runtime_host".to_string())
            .spawn(move || {
                rt::System::new("rest_api").block_on(async { run_rest_api("8086").await.unwrap() })
            })
            .unwrap();

        let test_run_1 = Builder::new_current_thread()
            .thread_name("test_thread_submission_controller_run_1")
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move { test_submission_controller_1.run().await.unwrap() });

        let test_run_2 = Builder::new_current_thread()
            .thread_name("test_thread_submission_controller_run_2")
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move { test_submission_controller_2.run().await.unwrap() });

        assert_eq!(test_run_1, expected_response);

        assert_eq!(test_run_2.id, expected_response.id);
        assert_eq!(test_run_2.status, expected_response.status);
        assert_eq!(test_run_2.message, expected_response.message);
        assert!(test_run_2.attempts >= 1 && test_run_2.attempts <= 10);
    }

    #[test]
    fn test_batch_submitter_task_handler_spawn() {
        let mock_batch = MockTrackingBatch::create("".to_string());
        let mock_batch_id = BatchTrackingId::create(mock_batch);
        // Test batch with echo endpoint
        let test_batch_envelope = BatchEnvelope {
            id: mock_batch_id.clone(),
            address: "http://127.0.0.1:8087/echo".to_string(),
            payload: vec![1, 1, 1, 1],
        };
        let expected_response =
            SubmissionResponse::new(mock_batch_id, 200, "\u{1}\u{1}\u{1}\u{1}".to_string(), 1);

        // Start REST API
        std::thread::Builder::new()
            .name("rest_api".to_string())
            .spawn(move || {
                rt::System::new("rest_api").block_on(async { run_rest_api("8087").await.unwrap() })
            })
            .unwrap();

        let (tx, rx): (
            std::sync::mpsc::Sender<BatchMessage<BatchTrackingId>>,
            std::sync::mpsc::Receiver<BatchMessage<BatchTrackingId>>,
        ) = std::sync::mpsc::channel();

        let new_task = NewTask::new(tx, test_batch_envelope);

        let runtime = Builder::new_current_thread()
            .thread_name("test_thread_task_handler")
            .enable_all()
            .build()
            .unwrap();

        std::thread::Builder::new()
            .name("test_thread".to_string())
            .spawn(move || {
                runtime.block_on(async move {
                    tokio::spawn(TaskHandler::spawn(new_task));
                    // Let the above task finish before dropping the runtime
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await
                })
            })
            .unwrap();

        let response = rx.recv().unwrap();

        assert_eq!(
            response,
            BatchMessage::SubmissionResponse(expected_response)
        );
    }

    struct MockObserver<T: TrackingId> {
        tx: std::sync::mpsc::Sender<(T, Option<u16>, Option<String>)>,
    }

    impl<T: TrackingId> SubmitterObserver<T> for MockObserver<T> {
        fn notify(&self, id: T, status: Option<u16>, message: Option<String>) {
            let _ = self.tx.send((id, status, message));
        }
    }

    #[test]
    fn test_batch_submitter_submitter_service() {
        let mock_batch = MockTrackingBatch::create("".to_string());
        let mock_batch_id = BatchTrackingId::create(mock_batch);
        // Start REST API
        std::thread::Builder::new()
            .name("rest_api".to_string())
            .spawn(move || {
                rt::System::new("rest_api").block_on(async { run_rest_api("8088").await.unwrap() })
            })
            .unwrap();

        let addresser = Box::new(BatchAddresser::new("http://127.0.0.1:8088/echo", None));

        let batch_queue = Box::new(
            vec![BatchSubmission {
                id: mock_batch_id.clone(),
                service_id: None,
                serialized_batch: vec![1, 1, 1, 1],
            }]
            .into_iter(),
        );

        // Channel to get what the mock observer receives
        let (tx, rx) = std::sync::mpsc::channel();

        let observer = Box::new(MockObserver { tx });

        let submitter = BatchSubmitter::start(addresser, batch_queue, observer).unwrap();

        // Listen for the response from the observer
        // First response is a notification that the submitter is processing the batch
        let first_response = rx.recv().unwrap();
        let first_expected_response: (BatchTrackingId, Option<u16>, Option<String>) =
            (mock_batch_id.clone(), Some(0), None);

        // Second response is the submission response
        let second_response = rx.recv().unwrap();
        let second_expected_response: (BatchTrackingId, Option<u16>, Option<String>) = (
            mock_batch_id.clone(),
            Some(200),
            Some("\u{1}\u{1}\u{1}\u{1}".to_string()),
        );

        submitter.shutdown().unwrap();

        assert_eq!(first_response, first_expected_response);
        assert_eq!(second_response, second_expected_response);
    }
}*/

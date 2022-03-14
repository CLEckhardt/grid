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

// SUBMITTER
//
// Manages the submission of the batches to the DLT. This implementation is async.

use super::{Addresser, BatchSubmission, Submitter, SubmitterObserver};
use crate::error::{ClientError, InternalError};
use log::{error, info};
use std::{fmt, iter::Iterator, thread, time};
use tokio::{runtime::Builder, sync::mpsc};

// Number of times a submitter task will retry submission in quick succession
const RETRY_ATTEMPTS: u16 = 10;
// Time the submitter will wait to repoll after receiving None, in milliseconds
const POLLING_INTERVAL: u64 = 1000;

#[derive(Debug, Clone, PartialEq)]
struct BatchEnvelope {
    id: String,
    address: String,
    payload: Vec<u8>,
}

#[allow(clippy::borrowed_box)] // Box must be borrowed to maintain proper ownership and lifetimes
impl BatchEnvelope {
    fn create(
        batch_submission: BatchSubmission,
        addresser: &Box<dyn Addresser + Send>,
    ) -> Result<Self, InternalError> {
        let address = addresser.address(batch_submission.service_id)?;
        Ok(Self {
            id: batch_submission.id,
            address,
            payload: batch_submission.serialized_batch,
        })
    }
}

#[derive(Debug, PartialEq)]
struct SubmissionResponse {
    id: String,
    status: u16,
    message: String,
}

impl SubmissionResponse {
    fn new(id: String, status: u16, message: String) -> Self {
        Self {
            id,
            status,
            message,
        }
    }
}

#[derive(Debug, PartialEq)]
struct SubmissionCommand {
    batch_envelope: BatchEnvelope,
    attempts: u16,
}

impl SubmissionCommand {
    fn new(batch_envelope: BatchEnvelope) -> Self {
        Self {
            batch_envelope,
            attempts: 0,
        }
    }

    async fn execute(&mut self) -> Result<SubmissionResponse, reqwest::Error> {
        let client = reqwest::Client::builder()
            .timeout(time::Duration::from_secs(15))
            .build()?;

        self.attempts += 1;

        let res = client
            .post(self.batch_envelope.address.clone())
            .body(self.batch_envelope.payload.clone())
            .send()
            .await?;

        Ok(SubmissionResponse::new(
            self.batch_envelope.id.clone(),
            res.status().as_u16(),
            res.text().await?,
        ))
    }
}

#[derive(Debug, PartialEq)]
struct SubmissionController {
    command: SubmissionCommand,
}

impl SubmissionController {
    fn new(batch_envelope: BatchEnvelope) -> Self {
        Self {
            command: SubmissionCommand::new(batch_envelope),
        }
    }

    async fn run(&mut self) -> Result<SubmissionResponse, ClientError> {
        let mut wait: u64 = 250;
        let mut response = self.command.execute().await;
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
                        break; // This error is returned outside of the loop and not behind a &
                    }
                }
            }
            wait += 500;
        }
        let res = response.map_err(ClientError::from)?;
        Ok(res)
    }
}

struct TaskHandler;

impl TaskHandler {
    async fn spawn(task: NewTask) {
        let id = task.batch_envelope.id.clone();
        let submission = SubmissionController::new(task.batch_envelope).run().await;

        let task_message = match submission {
            Ok(s) => BatchMessage::SubmissionResponse(s),
            Err(e) => BatchMessage::ErrorResponse(ErrorResponse {
                id,
                error: e.to_string(),
            }),
        };
        let _ = task.tx.send(task_message);
    }
}

struct NewTask {
    tx: std::sync::mpsc::Sender<BatchMessage>,
    batch_envelope: BatchEnvelope,
}

impl NewTask {
    fn new(tx: std::sync::mpsc::Sender<BatchMessage>, batch_envelope: BatchEnvelope) -> Self {
        Self { tx, batch_envelope }
    }
}

impl fmt::Debug for NewTask {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "{:?}", self.batch_envelope)
    }
}

#[derive(Debug)]
enum TerminateMessage {
    Terminate,
}

// Represents a message from the main thread
#[derive(Debug)]
enum CentralMessage {
    NewTask(NewTask),
    Terminate,
}

struct ErrorResponse {
    id: String,
    error: String,
}

// Represents a message from a task handler
enum BatchMessage {
    SubmissionNotification(String),
    SubmissionResponse(SubmissionResponse),
    ErrorResponse(ErrorResponse),
}

struct BatchSubmitter {
    runtime_handle: thread::JoinHandle<()>,
    leader_handle: thread::JoinHandle<()>,
    leader_tx: std::sync::mpsc::Sender<TerminateMessage>,
    listener_handle: thread::JoinHandle<()>,
    listener_tx: std::sync::mpsc::Sender<TerminateMessage>,
}

impl Submitter<'_> for BatchSubmitter {
    fn start<'a>(
        addresser: Box<dyn Addresser + Send>,
        mut queue: Box<dyn Iterator<Item = BatchSubmission> + Send>,
        observer: Box<dyn SubmitterObserver + Send>,
    ) -> Result<Box<Self>, InternalError> {
        // Create channels for for termination messages
        let (leader_tx, leader_rx) = std::sync::mpsc::channel();
        let (listener_tx, listener_rx) = std::sync::mpsc::channel();

        // Channel for messages from the async tasks to the sync listener thread
        let (tx_submission, rx_submission): (
            std::sync::mpsc::Sender<BatchMessage>,
            std::sync::mpsc::Receiver<BatchMessage>,
        ) = std::sync::mpsc::channel();
        // Clone the sender so that the leader thread can notify the listener thread that it will
        // start submitting a batch
        let tx_leader = tx_submission.clone();

        // Channel for messges from the leader thread to the task-spawning thread
        let (tx_spawner, mut rx_spawner) = mpsc::channel(64);

        // Create the runtime here to better catch errors on building
        let rt = Builder::new_multi_thread()
            .enable_all()
            .thread_name("submitter_async_runtime")
            .build()
            .map_err(|e| InternalError::with_message(format!("{:?}", e)))?;

        // Move the asnyc runtime to a separate thread so it doesn't block this one
        let runtime_handle = std::thread::Builder::new()
            .name("submitter_async_runtime_host".to_string())
            .spawn(move || {
                rt.block_on(async move {
                    while let Some(msg) = rx_spawner.recv().await {
                        match msg {
                            CentralMessage::NewTask(t) => {
                                tokio::spawn(TaskHandler::spawn(t));
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
                        Some(next_batch) => match BatchEnvelope::create(next_batch, &addresser) {
                            Ok(b) => {
                                info!("Batch {}: received from queue", &b.id);
                                if let Err(e) = tx_leader
                                    .send(BatchMessage::SubmissionNotification(b.id.clone()))
                                {
                                    error!("Error sending submission notification message: {:?}", e)
                                }
                                if let Err(e) = tx_spawner.blocking_send(CentralMessage::NewTask(
                                    NewTask::new(tx_submission.clone(), b),
                                )) {
                                    error!("Error sending NewTask message: {:?}", e)
                                };
                            }
                            Err(e) => error!("Error creating batch envelope: {:?}", e),
                        },
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
                            BatchMessage::SubmissionNotification(id) => {
                                observer.notify(id, Some(0), None) // 0 signifies pre-submission
                            }
                            BatchMessage::SubmissionResponse(s) => {
                                info!(
                                    "Batch {id}: received submission response [{code}]",
                                    id = &s.id,
                                    code = &s.status
                                );
                                // TODO: Log the number of submission attempts?
                                observer.notify(s.id, Some(s.status), Some(s.message))
                            }
                            BatchMessage::ErrorResponse(e) => {
                                error!("Submission error for batch {}: {:?}", &e.id, &e.error);
                                observer.notify(e.id, None, Some(e.error.to_string()));
                            }
                        }
                    }
                }
            })
            .map_err(|e| InternalError::with_message(format!("{:?}", e)))?;

        Ok(Box::new(Self {
            runtime_handle,
            leader_handle,
            leader_tx,
            listener_handle,
            listener_tx,
        }))
    }

    // TODO: Break this apart into shutdown trait
    fn shutdown(self) -> Result<(), InternalError> {
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

#[cfg(test)]
mod tests {

    use super::{super::addresser::BatchAddresser, *};
    // TODO: Figure out a way to write tests with API calls here

    #[test]
    fn test_batch_submitter_batch_envelope_create() {
        let test_addresser_wo_serv: Box<dyn Addresser + std::marker::Send> =
            Box::new(BatchAddresser::new("http://127.0.0.1:8080", None));
        let test_addresser_w_serv: Box<dyn Addresser + std::marker::Send> = Box::new(
            BatchAddresser::new("http://127.0.0.1:8080", Some("service_id")),
        );

        let test_batch_submission_wo_serv = BatchSubmission {
            id: "batch_without_service_id".to_string(),
            service_id: None,
            serialized_batch: vec![1, 1, 1, 1],
        };
        let test_batch_submission_w_serv = BatchSubmission {
            id: "batch_with_service_id".to_string(),
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
                id: "batch_without_service_id".to_string(),
                address: "http://127.0.0.1:8080".to_string(),
                payload: vec![1, 1, 1, 1],
            }
        );
        assert_eq!(
            BatchEnvelope::create(test_batch_submission_w_serv.clone(), &test_addresser_w_serv)
                .unwrap(),
            BatchEnvelope {
                id: "batch_with_service_id".to_string(),
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
        let response =
            SubmissionResponse::new("123-abc".to_string(), 200, "Everything is ok".to_string());

        assert_eq!(&response.id, &"123-abc".to_string());
        assert_eq!(&response.status, &200);
        assert_eq!(&response.message, &"Everything is ok".to_string());
    }

    #[test]
    fn test_batch_submitter_submission_command_new() {
        let test_batch_envelope = BatchEnvelope {
            id: "123-abc".to_string(),
            address: "http://127.0.0.1:8080".to_string(),
            payload: vec![1, 1, 1, 1],
        };
        let test_submission_command = SubmissionCommand::new(test_batch_envelope);
        assert_eq!(
            test_submission_command,
            SubmissionCommand {
                batch_envelope: BatchEnvelope {
                    id: "123-abc".to_string(),
                    address: "http://127.0.0.1:8080".to_string(),
                    payload: vec![1, 1, 1, 1],
                },
                attempts: 0,
            }
        )
    }

    #[test]
    fn test_batch_submitter_submission_controller_new() {
        let test_batch_envelope = BatchEnvelope {
            id: "123-abc".to_string(),
            address: "http://127.0.0.1:8080/echo".to_string(),
            payload: vec![1, 1, 1, 1],
        };
        let test_submission_controller = SubmissionController::new(test_batch_envelope);
        assert_eq!(
            test_submission_controller,
            SubmissionController {
                command: SubmissionCommand {
                    batch_envelope: BatchEnvelope {
                        id: "123-abc".to_string(),
                        address: "http://127.0.0.1:8080/echo".to_string(),
                        payload: vec![1, 1, 1, 1],
                    },
                    attempts: 0,
                }
            }
        )
    }
}

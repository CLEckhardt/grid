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
use tokio::{runtime::Builder, sync::mpsc};

use super::{Addresser, BatchSubmission, Submitter, SubmitterObserver};
use crate::error::{ClientError, InternalError};

// Number of times a submitter task will retry submission in quick succession
const RETRY_ATTEMPTS: u16 = 10;
// Time the submitter will wait to repoll after receiving None, in milliseconds
const POLLING_INTERVAL: u64 = 1000;

//
// OBJECTS THAT MOVE DATA THROUGH THE SUBMITTER

#[derive(Debug, Clone, PartialEq)]
// Wraps the batch as it moves through the submitter
struct BatchEnvelope {
    id: String,
    address: String,
    payload: Vec<u8>,
}

// Box must be borrowed to maintain proper ownership and lifetimes
#[allow(clippy::borrowed_box)]
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
// Carries the submission response from the http client back through the submitter to the observer
struct SubmissionResponse {
    id: String,
    status: u16,
    message: String,
    attempts: u16,
}

impl SubmissionResponse {
    fn new(id: String, status: u16, message: String, attempts: u16) -> Self {
        Self {
            id,
            status,
            message,
            attempts,
        }
    }
}

#[derive(Debug, PartialEq)]
// A message about a batch; sent between threads
enum BatchMessage {
    SubmissionNotification(String),
    SubmissionResponse(SubmissionResponse),
    ErrorResponse(ErrorResponse),
}

#[derive(Debug)]
// A message sent from the leader thread to the async runtime
enum CentralMessage {
    NewTask(NewTask),
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

#[derive(Debug, PartialEq)]
// Communicates an errror message from the task handler to the listener thread
struct ErrorResponse {
    id: String,
    error: String,
}

//
// SUBMITTER SUBCOMPONENTS

#[derive(Debug, PartialEq)]
// Responsible for executing the submission request
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
            self.attempts,
        ))
    }
}

#[derive(Debug, PartialEq)]
// Responsible for controlling retry behavior
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

/// The submission service.
///
/// The `BatchSubmitter` struct acts as a handle of sorts for the submission
/// service.
///
/// This implementation utilizes at least three additional threads, besides the
/// one on which it is initialized. It uses a tokio async runtime, which will
/// use more threads if they are available.
///
/// Because this implementation is async, batches may not be submitted in the
/// exact order in which they are received. Controlling the order and pace at
/// which batches are submitted should be done by the queue component.
pub struct BatchSubmitter {
    runtime_handle: thread::JoinHandle<()>,
    leader_handle: thread::JoinHandle<()>,
    leader_tx: std::sync::mpsc::Sender<TerminateMessage>,
    listener_handle: thread::JoinHandle<()>,
    listener_tx: std::sync::mpsc::Sender<TerminateMessage>,
}

impl Submitter<'_> for BatchSubmitter {
    /// Initialize the submission service.
    ///
    /// Note that the submitter consumes the addresser, queuer, and observer.
    /// These are each moved to separate threads within the submitter.
    fn start<'a>(
        addresser: Box<dyn Addresser + Send>,
        mut queue: Box<dyn Iterator<Item = BatchSubmission> + Send>,
        observer: Box<dyn SubmitterObserver + Send>,
    ) -> Result<Box<Self>, InternalError> {
        // Create channels for termination messages
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
                                // 0 signifies pre-submission
                                observer.notify(id, Some(0), None)
                            }
                            BatchMessage::SubmissionResponse(s) => {
                                info!(
                                    "Batch {id}: received submission response [{code}] after \
                                    {attempts} attempt(s)",
                                    id = &s.id,
                                    code = &s.status,
                                    attempts = &s.attempts
                                );
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
    use actix_web::{post, rt, App, HttpResponse, HttpServer, Responder};
    use std::time::{SystemTime, UNIX_EPOCH};

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
        let response = SubmissionResponse::new(
            "123-abc".to_string(),
            200,
            "Everything is ok".to_string(),
            1,
        );

        assert_eq!(&response.id, &"123-abc".to_string());
        assert_eq!(&response.status, &200);
        assert_eq!(&response.message, &"Everything is ok".to_string());
        assert_eq!(&response.attempts, &1);
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
    #[ignore]
    fn test_batch_submitter_submission_command_execute() {
        // Test batch with echo endpoint
        let test_batch_envelope = BatchEnvelope {
            id: "123-abc".to_string(),
            address: "http://127.0.0.1:8085/echo".to_string(),
            payload: vec![1, 1, 1, 1],
        };
        let mut test_submission_command = SubmissionCommand::new(test_batch_envelope);

        let expected_response = SubmissionResponse::new(
            "123-abc".to_string(),
            200,
            "\u{1}\u{1}\u{1}\u{1}".to_string(),
            1,
        );

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

    #[test]
    fn test_batch_submitter_submission_controller_run() {
        // Test batch with echo endpoint
        let test_batch_envelope_1 = BatchEnvelope {
            id: "123-abc".to_string(),
            address: "http://127.0.0.1:8086/echo".to_string(),
            payload: vec![1, 1, 1, 1],
        };
        let mut test_submission_controller_1 = SubmissionController::new(test_batch_envelope_1);

        // Test batch with echo_maybe endpoint to test retry behavior
        let test_batch_envelope_2 = BatchEnvelope {
            id: "123-abc".to_string(),
            address: "http://127.0.0.1:8086/echo_maybe".to_string(),
            payload: vec![1, 1, 1, 1],
        };
        let mut test_submission_controller_2 = SubmissionController::new(test_batch_envelope_2);

        let expected_response = SubmissionResponse::new(
            "123-abc".to_string(),
            200,
            "\u{1}\u{1}\u{1}\u{1}".to_string(),
            1,
        );

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
        // Test batch with echo endpoint
        let test_batch_envelope = BatchEnvelope {
            id: "123-abc".to_string(),
            address: "http://127.0.0.1:8087/echo".to_string(),
            payload: vec![1, 1, 1, 1],
        };
        let expected_response = SubmissionResponse::new(
            "123-abc".to_string(),
            200,
            "\u{1}\u{1}\u{1}\u{1}".to_string(),
            1,
        );

        // Start REST API
        std::thread::Builder::new()
            .name("rest_api".to_string())
            .spawn(move || {
                rt::System::new("rest_api").block_on(async { run_rest_api("8087").await.unwrap() })
            })
            .unwrap();

        let (tx, rx): (
            std::sync::mpsc::Sender<BatchMessage>,
            std::sync::mpsc::Receiver<BatchMessage>,
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

    struct MockObserver {
        tx: std::sync::mpsc::Sender<(String, Option<u16>, Option<String>)>,
    }

    impl SubmitterObserver for MockObserver {
        fn notify(&self, id: String, status: Option<u16>, message: Option<String>) {
            let _ = self.tx.send((id, status, message));
        }
    }

    #[test]
    fn test_batch_submitter_submitter_service() {
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
                id: "batch_without_service_id".to_string(),
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
        let first_expected_response: (String, Option<u16>, Option<String>) =
            ("batch_without_service_id".to_string(), Some(0), None);

        // Second response is the submission response
        let second_response = rx.recv().unwrap();
        let second_expected_response: (String, Option<u16>, Option<String>) = (
            "batch_without_service_id".to_string(),
            Some(200),
            Some("\u{1}\u{1}\u{1}\u{1}".to_string()),
        );

        submitter.shutdown().unwrap();

        assert_eq!(first_response, first_expected_response);
        assert_eq!(second_response, second_expected_response);
    }
}

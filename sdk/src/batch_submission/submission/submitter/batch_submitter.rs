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

use std::{fmt, time};

use crate::error::ClientError;
use crate::{
    batch_submission::{submission::url_resolver::UrlResolver, Submission},
    scope_id::ScopeId,
};

#[cfg(test)]
use crate::scope_id::GlobalScopeId;

// Number of times a submitter task will retry submission in quick succession
const RETRY_ATTEMPTS: u16 = 10;
// Time the submitter will wait to repoll after receiving None, in milliseconds
const POLLING_INTERVAL: u64 = 1000;

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
// Submitter subcomponents

#[async_trait]
trait ExecuteCommand<S: ScopeId> {
    async fn execute(&mut self) -> Result<SubmissionResponse<S>, reqwest::Error>;
}

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
}

#[async_trait]
impl<'a, S: ScopeId, R: 'a + UrlResolver<Id = S>> ExecuteCommand<S>
    for SubmissionCommand<'a, S, R>
{
    async fn execute(&mut self) -> Result<SubmissionResponse<S>, reqwest::Error> {
        let client = reqwest::Client::builder()
            .timeout(time::Duration::from_secs(15))
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

#[cfg(test)]
struct MockSubmissionCommand<S: ScopeId> {
    submission: Submission<S>,
    attempts: u16,
}

#[cfg(test)]
impl<S: ScopeId> MockSubmissionCommand<S> {
    fn new<'a, R: 'a + UrlResolver<Id = S>>(
        submission: Submission<S>,
        url_resolver: &'a R,
    ) -> Self {
        let _ = url_resolver;
        Self {
            submission,
            attempts: 0,
        }
    }
}

#[cfg(test)]
#[async_trait]
impl<S: ScopeId> ExecuteCommand<S> for MockSubmissionCommand<S> {
    async fn execute(&mut self) -> Result<SubmissionResponse<S>, reqwest::Error> {
        self.attempts += 1;
        if self.attempts < 3 {
            Ok(SubmissionResponse::new(
                "test".to_string(),
                self.submission.scope_id.clone(),
                503,
                "Busy".to_string(),
                self.attempts,
            ))
        } else {
            Ok(SubmissionResponse::new(
                "test".to_string(),
                self.submission.scope_id.clone(),
                200,
                "Success".to_string(),
                self.attempts,
            ))
        }
    }
}

#[derive(Debug, PartialEq)]
// Responsible for controlling retry behavior
struct SubmissionController;

impl SubmissionController {
    async fn run<S: ScopeId, C: ExecuteCommand<S>>(
        mut command: C,
    ) -> Result<SubmissionResponse<S>, ClientError> {
        let mut wait: u64 = 250;
        let mut response: Result<SubmissionResponse<S>, reqwest::Error> = command.execute().await;
        for _ in 1..RETRY_ATTEMPTS {
            match &response {
                Ok(res) => match &res.status {
                    200 => break,
                    503 => {
                        tokio::time::sleep(time::Duration::from_millis(wait)).await;
                        response = command.execute().await;
                    }
                    _ => break,
                },
                Err(e) => {
                    if e.is_timeout() {
                        tokio::time::sleep(time::Duration::from_millis(wait)).await;
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
    async fn spawn<'a, S: ScopeId, R: 'a + UrlResolver<Id = S>>(
        task: NewTask<S>,
        url_resolver: &'a R,
    ) {
        let batch_header = task.submission.batch_header().clone();
        let scope_id = task.submission.scope_id().clone();
        #[cfg(not(test))]
        let submission_command = SubmissionCommand::new(task.submission, url_resolver);

        // Inject mock_submission_command for testing
        // We should see if we can refine this - it means that the line above is never tested. It
        // is a worthwhile tradeoff for the ability to thoroughly test the other components.
        #[cfg(test)]
        let submission_command = MockSubmissionCommand::new(task.submission, url_resolver);

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

    // Required to manage lifetimes within the tests
    #[cfg(test)]
    async fn test_spawn(task: NewTask<GlobalScopeId>) {
        let mock_url_resolver = MockUrlResolver::new("test".to_string());
        Self::spawn(task, &mock_url_resolver).await
    }
}

#[cfg(test)]
#[derive(Debug, PartialEq)]
struct MockUrlResolver {
    url: String,
}

#[cfg(test)]
impl MockUrlResolver {
    fn new(url: String) -> Self {
        Self { url }
    }
}

#[cfg(test)]
impl UrlResolver for MockUrlResolver {
    type Id = GlobalScopeId;

    fn url(&self, scope_id: &GlobalScopeId) -> String {
        let _ = scope_id;
        format!("{}/test", &self.url)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scope_id::GlobalScopeId;
    use mockito;

    // Convenient mock submission for testing
    struct MockSubmission;

    impl MockSubmission {
        fn new() -> Submission<GlobalScopeId> {
            Submission {
                batch_header: "test".to_string(),
                scope_id: GlobalScopeId::new(),
                serialized_batch: vec![0, 0, 0, 0],
            }
        }
    }

    #[test]
    fn test_batch_submitter_submission_command_new() {
        let mock_submission = MockSubmission::new();
        let mock_url_resolver = MockUrlResolver::new("test.url".to_string());
        let test_command = SubmissionCommand::new(mock_submission.clone(), &mock_url_resolver);
        let expected_command = SubmissionCommand {
            submission: mock_submission,
            url_resolver: &mock_url_resolver,
            attempts: 0,
        };
        assert_eq!(test_command, expected_command);
    }

    // NOTE: This test is the only reason we need to import tokio_0_2 as long as we use reqwest
    // 0.10. Upon upgrading reqwest, this should use tokio 1.x.
    #[test]
    fn test_batch_submitter_submission_command_execute() {
        let url = mockito::server_url();
        let _m1 = mockito::mock("POST", "/test").with_body("success").create();
        let mock_submission = MockSubmission::new();
        let mock_url_resolver = MockUrlResolver::new(url);
        let mut test_command = SubmissionCommand::new(mock_submission, &mock_url_resolver);
        let expected_response = SubmissionResponse::new(
            "test".to_string(),
            GlobalScopeId::new(),
            200,
            "success".to_string(),
            1,
        );

        let response = tokio_0_2::runtime::Runtime::new()
            .unwrap()
            .block_on(async move { test_command.execute().await.unwrap() });

        assert_eq!(response, expected_response);
    }

    #[test]
    fn test_batch_submitter_submission_controller_run() {
        let mock_submission = MockSubmission::new();
        let mock_url_resolver = MockUrlResolver::new("throw-away-url".to_string());
        let mock_submission_command =
            MockSubmissionCommand::new(mock_submission, &mock_url_resolver);
        let expected_response = SubmissionResponse::new(
            "test".to_string(),
            GlobalScopeId::new(),
            200,
            "Success".to_string(),
            3,
        );
        let response = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async move {
                SubmissionController::run(mock_submission_command)
                    .await
                    .unwrap()
            });

        assert_eq!(response, expected_response);
    }

    #[test]
    fn test_batch_submitter_task_handler_spawn() {
        let mock_submission = MockSubmission::new();
        let expected_response = SubmissionResponse::new(
            "test".to_string(),
            GlobalScopeId::new(),
            200,
            "Success".to_string(),
            3,
        );
        let (tx, rx): (
            std::sync::mpsc::Sender<BatchMessage<GlobalScopeId>>,
            std::sync::mpsc::Receiver<BatchMessage<GlobalScopeId>>,
        ) = std::sync::mpsc::channel();
        let mock_new_task = NewTask::new(tx, mock_submission);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .thread_name("test_task_runtime")
            .enable_all()
            .build()
            .unwrap();
        let handle = std::thread::Builder::new()
            .name("test_task_runtime_thread".to_string())
            .spawn(move || {
                runtime.block_on(async move {
                    tokio::task::spawn(
                        TaskHandler::test_spawn(mock_new_task)
                    );
                    // Let the above task finish before dropping the runtime
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                });
            })
            .unwrap();
        let response = rx.recv().unwrap();
        let _ = handle.join();

        assert_eq!(
            response,
            BatchMessage::SubmissionResponse(expected_response)
        );
    }

    /*
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
    }*/
}

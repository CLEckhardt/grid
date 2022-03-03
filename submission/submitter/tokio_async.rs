use std::{thread, time};

use tokio::sync::{oneshot, mpsc};
use std::{thread, time};

const RETRY_ATTEMPTS: u16 = 10;

// Abstraction for a batch submission
// TODO: Change this to the real stuct
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BatchSubmission {
    url: String, // This is either sawtooth or splinter
    service_id: Option<String>,
    abstraction: String,
}

#[derive(Debug, PartialEq)]
struct SubmissionResponse {
    status: u16,
    message: String,
}

impl SubmissionResponse {
    fn new(status: u16, message: String) -> SubmissionResponse {
        SubmissionResponse {
            status: status,
            message: message,
        }
    }

    fn status(&self) -> &u16 {
        &self.status
    }

    fn message(&self) -> &String {
        &self.message
    }
}

#[derive(Debug, PartialEq)]
struct SubmissionCommand {
    url: String,
    service_id: Option<String>,
    payload: String,
    attempts: u16,
}

impl SubmissionCommand {
    fn new(url: String, service_id: Option<String>, payload: String) -> SubmissionCommand {
        SubmissionCommand {
            url: url,
            service_id: service_id,
            payload: payload,
            attempts: 0,
        }
    }

    fn execute(&mut self) -> SubmissionResponse {
        let client = reqwest::Client::new();
        let url = match &self.service_id {
            Some(s) => format!(
                "{url}?service_id={service_id}",
                url = self.url,
                service_id = s
            ),
            None => self.url.clone(),
        };
        self.attempts += 1;

        let res = client.post(url).body(self.payload.clone()).send().unwrap();
        // TODO: Remove the above unwrap with proper error handling
        // TODO: Remove the below unwrap too
        SubmissionResponse::new(res.status().as_u16(), res.text().unwrap())
    }

    fn attempts(&self) -> &u16 {
        &self.attempts
    }
}


#[derive(Debug, PartialEq)]
struct SubmissionController {
    command: SubmissionCommand,
}

impl SubmissionController {
    fn new(url: String, batch_submission: BatchSubmission) -> SubmissionController {
        SubmissionController {
            command: SubmissionCommand::new(
                batch_submission.url,
                batch_submission.service_id,
                batch_submission.abstraction,
            ),
        }
    }

    fn run(&mut self) -> SubmissionResponse {
        let mut wait: u64 = 250;
        let mut response = self.command.execute();
        for _ in 1..RETRY_ATTEMPTS {
            match &response.status {
                200 => break,
                503 => {
                    thread::sleep(time::Duration::from_millis(wait));
                    println!("wait: {}", wait);
                    response = self.command.execute();
                }
                _ => break,
            }
            wait += 500; // TODO: make this better
        }
        response
    }
}

struct SubmitterActor {
    tx: mpsc::Sender<ActorMessage>,
    rx: mpsc::Receiver<CentralMessage>,
}

impl SubmitterActor {
    fn start(tx: mpsc::Sender<ActorMessage>, rx: oneshot::Receiver<CentralMessage>) {
        let submitter_actor = SubmitterActor::new(tx, rx);
        submitter_actor.run();
    }

    fn new(tx: mpsc::Sender<ActorMessage>, rx: oneshot::Receiver<CentralMessage>) -> SubmitterActor {
        SubmitterActor { tx: tx, rx: rx }
    }

    async fn run(&self) {
        loop {
            let msg = self.rx.recv().await;

            match msg {
                CentralMessage::BatchSubmission(b) => {
                    let mut submission_controller = SubmissionController::new(b);
                    let submission_response = submission_controller.run();
                    self.tx
                        .send(ActorMessage::SubmissionResponse(submission_response));
                }
                CentralMessage::Terminate => break,
            }
        }
    }
}

struct SubmitterActorHandle {
    tx: , // 
    rx: , // 
}

#[derive(Debug, PartialEq)]
enum CentralMessage {
    BatchSubmission(BatchSubmission),
    Terminate,
}

#[derive(Debug, PartialEq)]
enum ActorMessage {
    SubmissionResponse(SubmissionResponse),
    Error, // This will be Error(_) in the future
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::sync::mpsc::{channel, sync_channel};
    use std::thread;

    #[test]
    fn test_batch_submitter_submission_response() {
        let response = SubmissionResponse::new(200, "Everything is ok".to_string());

        assert_eq!(response.status(), &200);
        assert_eq!(response.message(), &"Everything is ok".to_string());
    }

    #[test]
    fn test_batch_submitter_submission_command_new() {
        let test_submission_command = SubmissionCommand::new(
            "http://127.0.0.1:8080".to_string(),
            None,
            "batch".to_string(),
        );
        assert_eq!(
            test_submission_command,
            SubmissionCommand {
                url: "http://127.0.0.1:8080".to_string(),
                service_id: None,
                payload: "batch".to_string(),
                attempts: 0,
            }
        )
    }

    #[test]
    fn test_reqwest() {
        // requires a mock rest api to be running
        let client = reqwest::blocking::Client::new();
        let res = client
            .post("http://127.0.0.1:8080/echo?service_id=test")
            .body("hello world!")
            .send()
            .unwrap();
        assert_eq!(res.status().as_u16(), 200);
        assert_eq!(res.text().unwrap(), "hello world!".to_string());
    }

    #[test]
    fn test_batch_submitter_submission_command_execute() {
        let mut test_submission_command = SubmissionCommand::new(
            "http://127.0.0.1:8080/echo".to_string(),
            None,
            "batch".to_string(),
        );
        let test_submission_response = SubmissionResponse::new(200, "batch".to_string());
        assert_eq!(test_submission_command.execute(), test_submission_response);
    }

    #[test]
    //#[ignore = "not implemented"]
    fn test_batch_submitter_submission_controller_new() {
        let test_submission_controller = SubmissionController::new(BatchSubmission {
            url: "http://127.0.0.1:8080".to_string(),
            service_id: None,
            abstraction: "batch".to_string(),
        });
        assert_eq!(
            test_submission_controller,
            SubmissionController {
                command: SubmissionCommand {
                    url: "http://127.0.0.1:8080".to_string(),
                    service_id: None,
                    payload: "batch".to_string(),
                    attempts: 0,
                }
            }
        )
    }

    #[test]
    #[ignore = "testing another timing"]
    fn test_batch_submitter_submission_controller_run_w_503s() {
        let mut test_submission_controller = SubmissionController::new(BatchSubmission {
            url: "http://127.0.0.1:8080/echo_maybe".to_string(),
            service_id: None,
            abstraction: "batch".to_string(),
        });
        let test_submission_response = SubmissionResponse::new(200, "batch".to_string());
        assert_eq!(test_submission_controller.run(), test_submission_response);
    }

    #[test]
    fn test_batch_submitter_task_controller_new() {
        unimplemented!();
    }

    #[test]
    fn test_batch_submitter_submitter_actor() {
        // WORKING HERE
    }

}

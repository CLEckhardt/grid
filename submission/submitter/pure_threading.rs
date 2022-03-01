

// NOTE: The tests in this file require a mock rest API to be running on localhost


use std::sync::mpsc::{channel, sync_channel, Receiver, Sender};
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
        let url = match &self.service_id {
            Some(s) => format!(
                "{url}?service_id={service_id}",
                url = self.url,
                service_id = s
            ),
            None => self.url.clone(),
        };
        self.attempts += 1;

        let res = minreq::post(url)
            .with_body(self.payload.clone())
            .send()
            .unwrap();

        // TODO: Remove the above unwrap with proper error handling
        // TODO: Remove the below unwrap too
        SubmissionResponse::new(res.status_code as u16, res.as_str().unwrap().to_string())
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
    fn new(batch_submission: BatchSubmission) -> SubmissionController {
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

struct ThreadController {
    tx: Sender<ThreadMessage>,
    rx: Receiver<CentralMessage>,
}

impl ThreadController {
    fn start(tx: Sender<ThreadMessage>, rx: Receiver<CentralMessage>) {
        let thread_controller = ThreadController::new(tx, rx);
        thread_controller.run();
    }

    fn new(tx: Sender<ThreadMessage>, rx: Receiver<CentralMessage>) -> ThreadController {
        ThreadController { tx: tx, rx: rx }
    }

    fn run(&self) {
        loop {
            let msg = self.rx.recv().unwrap();

            match msg {
                CentralMessage::BatchSubmission(b) => {
                    let mut submission_controller = SubmissionController::new(b);
                    let submission_response = submission_controller.run();
                    self.tx
                        .send(ThreadMessage::SubmissionResponse(submission_response));
                }
                CentralMessage::Terminate => break,
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum CentralMessage {
    BatchSubmission(BatchSubmission),
    Terminate,
}

#[derive(Debug, PartialEq)]
enum ThreadMessage {
    SubmissionResponse(SubmissionResponse),
    Error, // This will be Error(_) in the future
}

#[cfg(test)]
mod tests {

    use crate::{
        BatchSubmission, CentralMessage, SubmissionCommand, SubmissionController,
        SubmissionResponse, ThreadController, ThreadMessage,
    };
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
    fn test_minreq() {
        // requires a mock rest api to be running
        let url = "http://127.0.0.1:8080/echo?service_id=test";
        let body = "hello world!";
        let res = minreq::post(url).with_body(body).send().unwrap();
        assert_eq!(res.status_code, 200);
        assert_eq!(res.as_str().unwrap(), "hello world!".to_string());
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
    fn test_batch_submitter_thread_controller_one_thread() {
        let (tx_central, rx_thread) = sync_channel(0);
        let (tx_thread, rx_central) = channel();
        let test_batch_submission = BatchSubmission {
            url: "http://127.0.0.1:8080/echo".to_string(),
            service_id: None,
            abstraction: "batch".to_string(),
        };
        let test_submission_response = SubmissionResponse::new(200, "batch".to_string());

        let handler = thread::spawn(move || {
            let test_thread_controller = ThreadController::start(tx_thread, rx_thread);
        });

        tx_central
            .send(CentralMessage::BatchSubmission(test_batch_submission))
            .unwrap();

        let received = rx_central.recv().unwrap();

        tx_central.send(CentralMessage::Terminate);

        handler.join().unwrap();

        assert_eq!(
            received,
            ThreadMessage::SubmissionResponse(test_submission_response)
        );
    }

    #[test]
    fn test_batch_submitter_thread_controller_two_threads() {
        let (tx_central_1, rx_thread_1) = sync_channel(0);
        let (tx_central_2, rx_thread_2) = sync_channel(0);
        let (tx_thread_1, rx_central) = channel();
        let tx_thread_2 = tx_thread_1.clone();

        let test_batch_submission_1 = BatchSubmission {
            url: "http://127.0.0.1:8080/late_echo_1".to_string(),
            service_id: None,
            abstraction: "batch_1".to_string(),
        };
        let test_batch_submission_2 = BatchSubmission {
            url: "http://127.0.0.1:8080/late_echo_2".to_string(),
            service_id: None,
            abstraction: "batch_2".to_string(),
        };
        let test_submission_response_1 = SubmissionResponse::new(200, "batch_1".to_string());
        let test_submission_response_2 = SubmissionResponse::new(200, "batch_2".to_string());

        let handler_1 = thread::spawn(move || {
            let test_thread_controller = ThreadController::start(tx_thread_1, rx_thread_1);
        });
        let handler_2 = thread::spawn(move || {
            let test_thread_controller = ThreadController::start(tx_thread_2, rx_thread_2);
        });

        tx_central_1
            .send(CentralMessage::BatchSubmission(test_batch_submission_1))
            .unwrap();
        tx_central_2
            .send(CentralMessage::BatchSubmission(test_batch_submission_2))
            .unwrap();

        let received_1 = rx_central.recv().unwrap();
        let received_2 = rx_central.recv().unwrap();

        tx_central_1.send(CentralMessage::Terminate);
        tx_central_2.send(CentralMessage::Terminate);

        handler_1.join().unwrap();
        handler_2.join().unwrap();

        assert_eq!(
            received_1,
            ThreadMessage::SubmissionResponse(test_submission_response_1)
        );
        assert_eq!(
            received_2,
            ThreadMessage::SubmissionResponse(test_submission_response_2)
        );
    }


}

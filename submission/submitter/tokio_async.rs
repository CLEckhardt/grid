use std::{fmt, iter::Iterator, thread, time};
use tokio::{runtime::Builder, sync::mpsc};

// Number of times a submitter task will retry submission in quick succession
const RETRY_ATTEMPTS: u16 = 10;
// Time the submitter will wait to repoll after receiving None, in milliseconds
const POLLING_INTERVAL: u64 = 1000;

// Error abstraction
// TODO: Replace uses of this with Grid error types
#[derive(PartialEq)]
struct AbstractError {
    message: String,
}

impl AbstractError {
    fn new(message: String) -> Self {
        Self { message }
    }
}

impl fmt::Display for AbstractError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Error: {}", self.message)
    }
}

impl fmt::Debug for AbstractError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Error: {}", self.message)
    }
}


// Abstraction for a batch submission
// TODO: Change this to the real stuct
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BatchSubmission {
    id: String,
    service_id: Option<String>,
    payload: String,
}


trait Addresser {
    fn address(&self, value: Option<String>) -> Result<String, AbstractError>;
}

#[derive(Debug, Clone, PartialEq)]
struct BatchAddresser {
    base_url: &'static str,
    parameter: Option<&'static str>,
}

impl BatchAddresser {
    fn new(base_url: &'static str, parameter: Option<&'static str>) -> Self {
        Self {
            base_url,
            parameter,
        }
    }
}

impl Addresser for BatchAddresser {
    fn address(&self, value: Option<String>) -> Result<String, AbstractError> {
        match &self.parameter {
            Some(p) => {
                if let Some(val) = value {
                    Ok(
                        format!(
                            "{base_url}?{parameter}={value}",
                            base_url = self.base_url.to_string(),
                            parameter = p.to_string(),
                            value = val
                        ), // TODO: Fix this unwrap!
                    )
                } else {
                    Err(AbstractError::new(
                        "Expecting service_id for batch but none was provided".to_string(),
                    ))
                }
            }
            None => {
                if value.is_none() {
                    Ok(self.base_url.to_string())
                } else {
                    Err(AbstractError::new(
                        "service_id for batch was provided but none was expected".to_string(),
                    ))
                }
            }
        }
    }
}


#[derive(Debug, Clone, PartialEq)]
struct BatchEnvelope {
    id: String,
    address: String,
    payload: String,
}

impl BatchEnvelope {
    fn create(
        batch_submission: BatchSubmission,
        addresser: &dyn Addresser,
    ) -> Result<Self, AbstractError> {
        let address = addresser.address(batch_submission.service_id)?;
        Ok(Self {
            id: batch_submission.id,
            address: address,
            payload: batch_submission.payload,
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

    async fn execute(&mut self) -> Result<SubmissionResponse, AbstractError> {
        // TODO: Make the errors in this return the reqwest error source
        let client = reqwest::Client::new();
        self.attempts += 1;

        let res = client
            .post(self.batch_envelope.address.clone())
            .body(self.batch_envelope.payload.clone())
            .send()
            .await
            .map_err(|_| AbstractError::new("reqwest error with post".to_string()))?;

        Ok(SubmissionResponse::new(
            self.batch_envelope.id.clone(),
            res.status().as_u16(),
            res.text()
                .await
                .map_err(|_| AbstractError::new("reqwest error with post".to_string()))?,
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

    async fn run(&mut self) -> Result<SubmissionResponse, AbstractError> {
        let mut wait: u64 = 250;
        let mut response = self.command.execute().await?;
        for _ in 1..RETRY_ATTEMPTS {
            match &response.status {
                200 => break,
                503 => {
                    tokio::time::sleep(time::Duration::from_millis(wait)).await;
                    println!("wait: {}", wait);
                    response = self.command.execute().await?;
                }
                _ => break,
            }
            wait += 500; // TODO: make this better
        }
        Ok(response)
    }
}


struct TaskHandler;

impl TaskHandler {
    async fn spawn(task: NewTask) {
        let submission = SubmissionController::new(task.batch_envelope).run().await;

        let task_message = match submission {
            Ok(s) => TaskMessage::SubmissionResponse(s),
            Err(e) => TaskMessage::Error(e),
        };
        // TODO: Figure out what we want to do with an error here, if we want to do anything
        // or figure out if it will resolve itself in the rest of Grid
        let _ = task.tx.send(task_message);
    }
}


struct NewTask {
    tx: std::sync::mpsc::Sender<TaskMessage>,
    batch_envelope: BatchEnvelope,
}

impl NewTask {
    fn new(tx: std::sync::mpsc::Sender<TaskMessage>, batch_envelope: BatchEnvelope) -> Self {
        Self { tx, batch_envelope }
    }
}

impl fmt::Debug for NewTask {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "{:?}", self.batch_envelope)
    }
}


// Represents a message from the main thread
#[derive(Debug)]
enum CentralMessage {
    NewTask(NewTask),
    Terminate, // TODO: Figure out if there's anything we need to do for shutdown behavior
}


// Represents a message from a task handler
enum TaskMessage {
    SubmissionResponse(SubmissionResponse),
    Error(AbstractError),
}


trait SubmitterObserver {
    fn notify(&self, id: String, status: Option<u16>, message: Option<String>);
}

// ONLY FOR PROTOTYPE
// TODO: Remove this
struct MockObserver;

impl SubmitterObserver for MockObserver {
    fn notify(&self, id: String, status: Option<u16>, message: Option<String>) {
        println!("ID: {id}", id = id);
        if let Some(s) = status {
            println!("Status: {status}", status = s);
        }
        if let Some(m) = message {
            println!("Message: {message}", message = m);
        }
    }
}


trait Submitter {
    fn run(&mut self, observer: Box<dyn SubmitterObserver + Send>);
}

struct BatchSubmitter<'a> {
    addresser: &'a dyn Addresser,
    queue: Box<dyn Iterator<Item = BatchSubmission>>,
}

impl<'a> BatchSubmitter<'a> {
    fn new<'b>(
        addresser: &'a dyn Addresser,
        queue: Box<dyn Iterator<Item = BatchSubmission>>,
    ) -> Self {
        Self { addresser, queue }
    }

    fn poll_for_batch(&mut self) -> BatchSubmission {
        let mut next_batch = self.queue.next();
        while next_batch.is_none() {
            thread::sleep(time::Duration::from_millis(POLLING_INTERVAL));
            next_batch = self.queue.next();
        }
        // TODO: Fix this unwrap
        next_batch.unwrap()
    }
}

impl<'a> Submitter for BatchSubmitter<'a> {
    fn run(&mut self, observer: Box<dyn SubmitterObserver + Send>) {
        // Channel for messages from the async tasks to the sync listener thread
        let (tx_task, rx_task): (
            std::sync::mpsc::Sender<TaskMessage>,
            std::sync::mpsc::Receiver<TaskMessage>,
        ) = std::sync::mpsc::channel();
        // Channel for messges from this main thread to the task-spawning thread
        let (tx_spawner, mut rx_spawner) = mpsc::channel(64);

        // Create the runtime here to better catch errors on building
        let rt = Builder::new_multi_thread()
            .enable_all()
            .thread_name("runtime")
            .build()
            .unwrap(); // TODO: Fix this unwrap

        // Move the asnyc runtime to a separate thread so it doesn't block this one
        std::thread::spawn(move || {
            rt.block_on(async move {
                while let Some(msg) = rx_spawner.recv().await {
                    match msg {
                        CentralMessage::NewTask(t) => {
                            tokio::spawn(TaskHandler::spawn(t));
                        }
                        CentralMessage::Terminate => break, // TODO: Do we need this?
                    }
                }
            })
        });

        // Set up and run the listener thread
        std::thread::spawn(move || {
            while let Ok(msg) = rx_task.recv() {
                match msg {
                    TaskMessage::SubmissionResponse(s) => {
                        observer.notify(s.id, Some(s.status), Some(s.message))
                    }
                    // TODO: Figure out error mapping here
                    TaskMessage::Error(e) => println!("ERROR: {:?}", e),
                };
            }
        });

        // Poll for batches and submit them
        loop {
            // poll_for_batch() blocks until a new batch is available
            if let Ok(next_batch) = BatchEnvelope::create(self.poll_for_batch(), self.addresser) {
                let _ = tx_spawner
                    .blocking_send(CentralMessage::NewTask(NewTask::new(
                        tx_task.clone(),
                        next_batch,
                    )))
                    .map_err(|err| println!("Log this error!! {:?}", err));
            } else {
                // TODO: Do something helpful here!
            }
        }
    }
}


fn main() {
    // Mock addresser
    let addresser = BatchAddresser::new("http://127.0.0.1:8080/echo", Some("service_id"));

    // Mock queue
    let batch_queue = Box::new(
        vec![
            BatchSubmission {
                id: "batch_1".to_string(),
                service_id: Some("123-abc".to_string()),
                payload: "batch_1 payload".to_string(),
            },
            BatchSubmission {
                id: "batch_2".to_string(),
                service_id: Some("123-abc".to_string()),
                payload: "batch_2 payload".to_string(),
            },
            BatchSubmission {
                id: "batch_3".to_string(),
                service_id: Some("123-abc".to_string()),
                payload: "batch_3 payload".to_string(),
            },
            BatchSubmission {
                id: "batch_4".to_string(),
                service_id: Some("123-abc".to_string()),
                payload: "batch_4 payload".to_string(),
            },
            BatchSubmission {
                id: "batch_5".to_string(),
                service_id: Some("123-abc".to_string()),
                payload: "batch_5 payload".to_string(),
            },
        ]
        .into_iter(),
    );

    // Mock observer
    let observer = Box::new(MockObserver {});

    let mut submitter = BatchSubmitter::new(&addresser, batch_queue);
    submitter.run(observer);

}


#[cfg(test)]
mod tests {

    use super::*;
    use std::sync::mpsc::{channel, sync_channel};
    use std::thread;

    #[test]
    fn test_batch_submitter_batch_addresser_new() {
        let expected_addresser_wo_serv = BatchAddresser {
            base_url: "http://127.0.0.1:8080",
            parameter: None,
        };
        let expected_addresser_w_serv = BatchAddresser {
            base_url: "http://127.0.0.1:8080",
            parameter: Some("service_id"),
        };

        assert_eq!(
            BatchAddresser::new("http://127.0.0.1:8080", None),
            expected_addresser_wo_serv
        );
        assert_eq!(
            BatchAddresser::new("http://127.0.0.1:8080", Some("service_id")),
            expected_addresser_w_serv
        );
    }

    #[test]
    fn test_batch_submitter_batch_addresser() {
        let test_addresser_wo_serv = BatchAddresser::new("http://127.0.0.1:8080", None);
        let test_addresser_w_serv =
            BatchAddresser::new("http://127.0.0.1:8080", Some("service_id"));

        assert_eq!(
            test_addresser_wo_serv.address(None),
            Ok("http://127.0.0.1:8080".to_string())
        );
        assert_eq!(
            test_addresser_w_serv.address(Some("123-abc".to_string())),
            Ok("http://127.0.0.1:8080?service_id=123-abc".to_string())
        );
        assert!(test_addresser_wo_serv
            .address(Some("123-abc".to_string()))
            .is_err());
        assert!(test_addresser_w_serv.address(None).is_err());
    }

    #[test]
    fn test_batch_submitter_batch_envelope_create() {
        let test_addresser_wo_serv = BatchAddresser::new("http://127.0.0.1:8080", None);
        let test_addresser_w_serv =
            BatchAddresser::new("http://127.0.0.1:8080", Some("service_id"));

        let test_batch_submission_wo_serv = BatchSubmission {
            id: "batch_without_service_id".to_string(),
            service_id: None,
            payload: "batch".to_string(),
        };
        let test_batch_submission_w_serv = BatchSubmission {
            id: "batch_with_service_id".to_string(),
            service_id: Some("123-abc".to_string()),
            payload: "batch".to_string(),
        };

        assert_eq!(
            BatchEnvelope::create(
                test_batch_submission_wo_serv.clone(),
                &test_addresser_wo_serv
            ),
            Ok(BatchEnvelope {
                id: "batch_without_service_id".to_string(),
                address: "http://127.0.0.1:8080".to_string(),
                payload: "batch".to_string(),
            })
        );
        assert_eq!(
            BatchEnvelope::create(test_batch_submission_w_serv.clone(), &test_addresser_w_serv),
            Ok(BatchEnvelope {
                id: "batch_with_service_id".to_string(),
                address: "http://127.0.0.1:8080?service_id=123-abc".to_string(),
                payload: "batch".to_string(),
            })
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
            payload: "batch".to_string(),
        };
        let test_submission_command = SubmissionCommand::new(test_batch_envelope);
        assert_eq!(
            test_submission_command,
            SubmissionCommand {
                batch_envelope: BatchEnvelope {
                    id: "123-abc".to_string(),
                    address: "http://127.0.0.1:8080".to_string(),
                    payload: "batch".to_string(),
                },
                attempts: 0,
            }
        )
    }

    #[test]
    fn test_reqwest() {
        // requires a mock rest api to be running
        let client = reqwest::Client::new();
        let rt = Builder::new_multi_thread()
            .enable_all()
            .thread_name("runtime")
            .build()
            .unwrap();
        let res = rt
            .block_on(
                client
                    .post("http://127.0.0.1:8080/echo?service_id=test")
                    .body("hello world!")
                    .send(),
            )
            .unwrap();
        let res_status = res.status().as_u16();
        let res_text = rt.block_on(res.text()).unwrap();
        assert_eq!(res_status, 200);
        assert_eq!(res_text, "hello world!".to_string());
    }

    #[test]
    fn test_batch_submitter_submission_command_execute() {
        let test_batch_envelope = BatchEnvelope {
            id: "123-abc".to_string(),
            address: "http://127.0.0.1:8080/echo".to_string(),
            payload: "batch".to_string(),
        };
        let mut test_submission_command = SubmissionCommand::new(test_batch_envelope);
        let rt = Builder::new_multi_thread()
            .enable_all()
            .thread_name("runtime")
            .build()
            .unwrap();
        let test_submission_response =
            SubmissionResponse::new("123-abc".to_string(), 200, "batch".to_string());
        assert_eq!(
            rt.block_on(test_submission_command.execute()),
            Ok(test_submission_response)
        );
    }

    #[test]
    fn test_batch_submitter_submission_controller_new() {
        let test_batch_envelope = BatchEnvelope {
            id: "123-abc".to_string(),
            address: "http://127.0.0.1:8080/echo".to_string(),
            payload: "batch".to_string(),
        };
        let mut test_submission_controller = SubmissionController::new(test_batch_envelope);
        assert_eq!(
            test_submission_controller,
            SubmissionController {
                command: SubmissionCommand {
                    batch_envelope: BatchEnvelope {
                        id: "123-abc".to_string(),
                        address: "http://127.0.0.1:8080/echo".to_string(),
                        payload: "batch".to_string(),
                    },
                    attempts: 0,
                }
            }
        )
    }
    /*
    #[test]
    #[ignore = "testing another timing"]
    fn test_batch_submitter_submission_controller_run_w_503s() {
        let mut test_submission_controller = SubmissionController::new(BatchSubmission {
            url: "http://127.0.0.1:8080/echo_maybe".to_string(),
            service_id: None,
            abstraction: "batch".to_string(),
        });
        let test_submission_response = SubmissionResponse::new(200, "batch".to_string());
        assert_eq!(
            block_on(test_submission_controller.run()),
            test_submission_response
        );
    }

    #[test]
    fn test_batch_submitter_dlt_url_builder() {
        let dlt_1 = DltDetail::new("http://test-url.com", None);
        let dlt_2 = DltDetail::new("http://test-url-param.com", Some("service_id"));

        assert_eq!(
            dlt_1.build_url("".to_string()),
            "http://test-url.com".to_string()
        );
        assert_eq!(
            dlt_2.build_url("123-abc".to_string()),
            "http://test-url-param.com?service_id=123-abc".to_string()
        );
    }*/
}


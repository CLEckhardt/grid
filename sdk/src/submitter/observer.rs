use super::SubmitterObserver;
use crate::batch_tracking::store::BatchTrackingStore;

#[allow(dead_code)]
pub struct BatchTrackingObserver {
    store: Box<dyn BatchTrackingStore>,
}

#[allow(unused_variables)]
impl SubmitterObserver for BatchTrackingObserver {
    fn notify(&self, id: String, status: Option<u16>, message: Option<String>) {
        unimplemented!();
        /*
        if let Some(s) = status {
            // TODO: Do we need to log any of these?
            match &s {
                // TODO: Need these methods
                &0 => self.store.update_batch_to_submitted(id),
                &200 => self.store.update_batch_submission_successful(id),
                // TODO: For the below, these should include a change_batch_to_unsubmitted() action
                &503 => self.store.update_batch_submission_busy(id), // include message?
                &404 => self.store.update_batch_submission_missing(id, message),
                &500 => self.store.update_batch_submission_internal_error(id), // include message?
                _ => self.store.update_batch_submission_unrecognized_status(id, status, message),
            }
        } else {
            // A missing status code represents an error in the submission process
            self.store.update_batch_dlt_error(id, message);
            todo!(); // Determine if the observer should notify another component
            // This error will have already been logged by the submitter
        }*/
    }
}

// STORE METHODS:
//
// store.update_batch_submission_successful()
//      - adds an entry for the submission
//      - the queuer will have updated the batch submission status to "submitted" already to
//          avoid timing issues for itself, so we don't need to do that here
// store.update_batch_submission_busy()
//      - adds an entry to the Submission table with error type "Busy"
//      - changes the batch from submitted to unsubmitted?
//      - changes the batch status to "Delayed"?
// store.update_batch_submission_missing()
//      - adds an entry to the Submission table with error type "Missing"
//      - changes the batch from submitted to unsubmitted?
//      - changes the batch status to "Delayed"?
// store.update_batch_submission_internal_error()
//      - adds an entry to the Submission table with error type "InternalError" and error message
//          passed through
//      - changes the batch from submitted to unsubmitted?
//      - changes the batch status to "Delayed"?
// store.update_batch_submission_unrecognized_status()
//      - adds an entry to the Submission table with error type "UnrecognizedStatus" and status
//      code and message passed through
//      - changes the batch from submitted to unsubmitted?
//      - changes the batch status to "Delayed"?
//
// A batch status of "Delayed" means that the queuer should retry these after a designated amount
// of time
//
// store.update_batch_dlt_error()
//      - records an error in the proper place in the database (where?)

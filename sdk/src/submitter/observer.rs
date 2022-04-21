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

//! Interprets messages from the submitter about batches and updates the store accordingly.

use super::{SubmitterObserver, TrackingId};
use crate::batch_tracking::store::BatchTrackingStore;

#[allow(dead_code)]
pub struct BatchTrackingObserver {
    store: Box<dyn BatchTrackingStore>,
}

#[allow(unused_variables)]
impl<T: TrackingId> SubmitterObserver<T> for BatchTrackingObserver {
    fn notify(&self, id: T, status: Option<u16>, message: Option<String>) {
        if let Some(s) = status {
            match &s {
                0 => {
                    unimplemented!()
                    // The batch has been accepted by the submitter and is in process of being
                    // submitted
                    // 1. Update batch entry to Submitted = true
                }
                200 => {
                    unimplemented!()
                    // Sucessful batch submission
                    // 1. Update the batch status to Pending
                    // 2. Add/update the submission entry
                    // 3. Log submission?
                }
                // TODO: For the below, these should include a change_batch_to_unsubmitted() action
                503 => {
                    unimplemented!()
                    // DLT was busy
                    // 1. Update the batch status to Delayed
                    // 2. Update the batch to Submitted = false
                    // 3. Add/update the submission entry
                    // 4. Add the DLT error
                    // 5. Log submission attempt
                }
                404 => {
                    unimplemented!()
                    // DLT was was not found; we will assume this is temporary
                    // 1. Update the batch status to Delayed
                    // 2. Update the batch to Submitted = false
                    // 3. Add/update the submission entry
                    // 4. Add the DLT error
                    // 5. Log submission attempt
                }
                500 => {
                    unimplemented!()
                    // DLT experienced an internal error; we will assume this is temporary
                    // 1. Update the batch status to Delayed
                    // 2. Update the batch to Submitted = false
                    // 3. Add/update the submission entry
                    // 4. Add the DLT error
                    // 5. Log submission attempt and error message
                }
                _ => {
                    unimplemented!()
                    // Unrecognized response; we will assume this is temporary
                    // 1. Update the batch status to Delayed
                    // 2. Update the batch to Submitted = false
                    // 3. Add/update the submission entry
                    // 4. Add the DLT error
                    // 5. Log submission attempt, status code, and error message
                }
            }
        } else {
            // A missing status code represents an error in the submission process
            // This error will have already been logged by the submitter
            unimplemented!(); // Determine if the observer should notify another component
        }
    }
}

// A batch status of "Delayed" means that the queuer should retry these after a designated amount
// of time

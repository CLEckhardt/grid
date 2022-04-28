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

//! Submitter traits and implementations for submitting batches to a DLT

use crate::{batch_tracking::store::TrackingBatch, error::InternalError};

// Stuff is probably not in the right place
use crate::submitter::batches::VerifiedBatch;

pub mod addresser;
pub mod batch_submitter;
pub mod observer;
pub mod batches;

/// An interface for generating the address to which a batch will be sent
///
/// If the DLT takes a parameter like `service_id`, `routing` will be the
/// `service_id` associated with that batch. If the DLT does not take a
/// parameter, `routing` will always be `None`.
pub trait Addresser/*<B: VerifiedBatch>*/: Send {
    type Batch: VerifiedBatch;
    /// Generate an address (i.e. URL) to which the batch will be sent.
    fn address(&self, batch: Self::Batch) -> String;
}

/// An interface for interpretting and recording updates from the submitter
///
/// Note: The implementation of the observer is tied to the implementation of the tracking ID
pub trait SubmitterObserver {
    type Id: TrackingId;
    /// Notify the observer of an update. The interpretation and recording
    /// of the update is determined by the observer's implementation.
    fn notify(&self, id: Self::Id, status: Option<u16>, message: Option<String>);
}

pub trait SubmitterBuilder<
    T: 'static + TrackingId,
    //B: VerifiedBatch,
    A: Addresser<Batch = dyn VerifiedBatch> + Send,
    //Q: Iterator<Item = dyn BatchSubmission<Submission = S>> + Send,
    Q: Iterator<Item = dyn VerifiedBatch> + Send,
    O: SubmitterObserver + Send,
>
{
    type RunnableSubmitter: RunnableSubmitter<T, A, Q, O>;

    fn new() -> Self;

    fn with_addresser(&mut self, addresser: A);

    fn with_queue(&mut self, queue: Q);

    fn with_observer(&mut self, observer: O);

    fn build(self) -> Result<Self::RunnableSubmitter, InternalError>;
}

pub trait RunnableSubmitter<
    T: 'static + TrackingId,
    //B: VerifiedBatch,
    //A: Addresser<B> + Send,
    A: Addresser<Batch = dyn VerifiedBatch> + Send,
    //Q: Iterator<Item = dyn BatchSubmission<Submission = S>> + Send,
    Q: Iterator<Item = dyn VerifiedBatch> + Send,
    O: SubmitterObserver + Send,
>
{
    type RunningSubmitter: RunningSubmitter;

    fn run(self) -> Result<Self::RunningSubmitter, InternalError>;
}

pub trait RunningSubmitter {
    fn signal_shutdown(&self) -> Result<(), InternalError>;

    /// Wind down and stop the submission service.
    fn shutdown(self) -> Result<(), InternalError>;
}

/// A generic representation of batch tracking information
///
/// This interface allows for different implementations of batch tracking information and metadata
/// to flow through the submitter. `TrackingId`s are created in the queuer and interpretted in
/// the observer.
pub trait TrackingId:
    Clone + std::fmt::Debug + std::fmt::Display + PartialEq + Sync + Send
{
    type Id;
    /// Create the identifier.
    fn create(batch: TrackingBatch) -> Self;

    /// Get the value of the identifier, consuming the object.
    fn get(self) -> Self::Id;
}

pub trait WithServiceId { 
    // This is a marker trait
}


// TODO: Make a BasicTrackingId that will work with all this
// Observer implementation will take this struct




/// A batch tracking ID containing the `batch_header` and `service_id`
#[derive(Clone, Debug, PartialEq)]
pub struct BatchTrackingId {
    batch_header: String,
    service_id: String,
}

impl std::fmt::Display for BatchTrackingId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "(batch_header: {}, service_id: {})",
            self.batch_header, self.service_id
        )
    }
}

impl TrackingId for BatchTrackingId {
    type Id = (String, String);

    /// Create a new `BatchTrackingId`.
    fn create(batch: TrackingBatch) -> Self {
        Self {
            batch_header: batch.batch_header().to_string(),
            service_id: batch.service_id().to_string(),
        }
    }

    /// Get the values of the `BatchTrackingId`, returned as a tuple.
    fn get(self) -> (String, String) {
        (self.batch_header, self.service_id)
    }
}
/*
#[derive(Clone, Debug, PartialEq)]
pub struct BatchTrackingIdNoSID {
    batch_header: String,
}

impl std::fmt::Display for BatchTrackingIdNoSID {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "(batch_header: {})",
            self.batch_header
        )
    }
}

impl TrackingId for BatchTrackingIdNoSID {
    type Id = String;

    /// Create a new `BatchTrackingId`.
    fn create(batch: TrackingBatch) -> Self {
        Self {
            batch_header: batch.batch_header().to_string(),
        }
    }

    /// Get the values of the `BatchTrackingId`, returned as a tuple.
    fn get(self) -> String {
        self.batch_header
    }
}*/

/*
pub trait BatchSubmission {
    type Submission;
    fn create(batch: TrackingBatch) -> Self;

    fn get(self) -> Self::Submission;
}



#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SubmissionWithSID<T: TrackingId + WithServiceId> {
    id: T,
    service_id: String,
    serialized_batch: Vec<u8>,
}

impl<T: TrackingId + WithServiceId> BatchSubmission for SubmissionWithSID<T> {
    type Submission = (T, String, Vec<u8>);
    fn create(batch: TrackingBatch) -> Self {
        SubmissionWithSID {
            id: T::create(&batch),
            service_id: batch.service_id(),
            serialized_batch: batch.serialized_batch(),
        }
    }

    fn get(self) -> (T, String, Vec<u8>) {
        (self.id, self.service_id, self.serialized_batch)
    }
}



impl<T: TrackingId + WithServiceId> SubmissionWithSID<T> {

    pub fn id(&self) -> &T {
        &self.id
    }

    /// Return the batch's associated `service_id`.
    pub fn service_id(&self) -> &String {
        &self.service_id
    }

    /// Return the serialized batch.
    pub fn serialized_batch(&self) -> &Vec<u8> {
        &self.serialized_batch
    }
}*/

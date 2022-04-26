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

pub mod addresser;
pub mod batch_submitter;
pub mod observer;

/// An interface for generating the address to which a batch will be sent
///
/// If the DLT takes a parameter like `service_id`, `routing` will be the
/// `service_id` associated with that batch. If the DLT does not take a
/// parameter, `routing` will always be `None`.
pub trait Addresser: Send {
    /// Generate an address (i.e. URL) to which the batch will be sent.
    fn address(&self, routing: Option<String>) -> Result<String, InternalError>;
}

/// An interface for interpretting and recording updates from the submitter
pub trait SubmitterObserver<T: TrackingId> {
    /// Notify the observer of an update. The interpretation and recording
    /// of the update is determined by the observer's implementation.
    fn notify(&self, id: T, status: Option<u16>, message: Option<String>);
}

pub trait SubmitterBuilder<
    T: 'static + TrackingId,
    A: Addresser + Send,
    Q: Iterator<Item = BatchSubmission<T>> + Send,
    O: SubmitterObserver<T> + Send,
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
    A: Addresser + Send,
    Q: Iterator<Item = BatchSubmission<T>> + Send,
    O: SubmitterObserver<T> + Send,
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

/// A batch submission
///
/// Represents the minimum information required to submit a batch, including:
///
/// * tracking information (the batch's `id`)
/// * routing information (the `service_id` to which the batch should be
///   applied, if applicable)
/// * the batch itself
///
/// A queue will typically create a `BatchSubmission` object and deliver it to
/// the submitter via the queue's `next()` method.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BatchSubmission<T: TrackingId> {
    id: T,
    service_id: Option<String>,
    serialized_batch: Vec<u8>,
}

impl<T: TrackingId> BatchSubmission<T> {
    /// Create a new BatchSubmission.
    pub fn new(id: T, service_id: Option<String>, serialized_batch: Vec<u8>) -> Self {
        Self {
            id,
            service_id,
            serialized_batch,
        }
    }

    pub fn id(&self) -> &T {
        &self.id
    }

    /// Return the batch's associated `service_id`.
    pub fn service_id(&self) -> &Option<String> {
        &self.service_id
    }

    /// Return the serialized batch.
    pub fn serialized_batch(&self) -> &Vec<u8> {
        &self.serialized_batch
    }
}

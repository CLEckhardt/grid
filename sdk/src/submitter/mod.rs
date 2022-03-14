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

use crate::error::InternalError;

pub mod addresser;
pub mod batch_submitter;
pub mod observer;

/// An interface for generating the address to which a batch will be sent
///
/// If the DLT takes a parameter like `service_id`, `routing` will be the
/// `service_id` associated with that batch. If the DLT does not take a
/// parameter, `routing` will always be `None`.
pub trait Addresser {
    /// Generate an address (i.e. URL) to which the batch will be sent.
    fn address(&self, routing: Option<String>) -> Result<String, InternalError>;
}

/// An interface for interpretting and recording updates from the submitter
pub trait SubmitterObserver {
    /// Notify the observer of an update. The interpretation and recording
    /// of the update is determined by the observer's implementation.
    fn notify(&self, id: String, status: Option<u16>, message: Option<String>);
}

/// An interface to a submission service
///
/// This interface acts as a handle to the submission service. Note that the
/// submitter consumes the addesser, queue, and observer.
pub trait Submitter<'a> {
    /// Start the submission service. Return any error that occurs while
    /// initializing its implementation.
    fn start(
        addresser: Box<dyn Addresser + Send>,
        queue: Box<dyn Iterator<Item = BatchSubmission> + Send>,
        observer: Box<dyn SubmitterObserver + Send>,
    ) -> Result<Box<Self>, InternalError>;

    /// Wind down and stop the submission service.
    fn shutdown(self) -> Result<(), InternalError>;
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
pub struct BatchSubmission {
    id: String,
    service_id: Option<String>,
    serialized_batch: Vec<u8>,
}

impl BatchSubmission {
    /// Create a new BatchSubmission.
    pub fn new(id: String, service_id: Option<String>, serialized_batch: Vec<u8>) -> Self {
        Self {
            id,
            service_id,
            serialized_batch,
        }
    }

    /// Return the batch's `id`.
    pub fn id(&self) -> &String {
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

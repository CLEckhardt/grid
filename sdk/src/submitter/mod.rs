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
// Stuff is probably not in the right place
use crate::submitter::batches::{BatchEnvelope, TrackingId, VerifiedBatch};

pub mod addresser;
pub mod batch_submitter;
pub mod batches;
pub mod observer;

/// An interface for generating the address to which a batch will be sent
///
/// If the DLT takes a parameter like `service_id`, `routing` will be the
/// `service_id` associated with that batch. If the DLT does not take a
/// parameter, `routing` will always be `None`.
pub trait Addresser: Send {
    type Batch: VerifiedBatch;
    /// Generate an address (i.e. URL) to which the batch will be sent.
    fn address(&self, batch: &Self::Batch) -> String;
}

/// An interface for interpretting and recording updates from the submitter
pub trait SubmitterObserver<T: TrackingId> {
    /// Notify the observer of an update. The interpretation and recording
    /// of the update is determined by the observer's implementation.
    fn notify(&self, id: T, status: Option<u16>, message: Option<String>);
}

pub trait SubmitterBuilder<
    T: 'static + TrackingId,
    Q: Iterator<Item = BatchEnvelope<T>> + Send,
    O: SubmitterObserver<T> + Send,
>
{
    type RunnableSubmitter: RunnableSubmitter<T, Q, O>;

    fn new() -> Self;

    fn with_queue(&mut self, queue: Q);

    fn with_observer(&mut self, observer: O);

    fn build(self) -> Result<Self::RunnableSubmitter, InternalError>;
}

pub trait RunnableSubmitter<
    T: 'static + TrackingId,
    Q: Iterator<Item = BatchEnvelope<T>> + Send,
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

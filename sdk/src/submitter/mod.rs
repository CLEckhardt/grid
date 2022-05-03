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
use crate::submitter::batches::Submission;

pub mod addresser;
pub mod batch_submitter;
pub mod batches;
pub mod observer;

//
// Move this somewhere else
//
#[derive(Clone, Debug, PartialEq)]
struct FullyQualifiedServiceId {}

pub trait ScopeId: Clone + PartialEq + std::fmt::Debug + Sync + Send {}

#[derive(Clone, Debug, PartialEq)]
pub struct GlobalScopeId {}
impl ScopeId for GlobalScopeId {}

#[derive(Clone, Debug, PartialEq)]
pub struct ServiceScopeId {
    service_id: FullyQualifiedServiceId,
}
impl ScopeId for ServiceScopeId {}

//



/// An interface for generating the address to which a batch will be sent
///
/// If the DLT takes a parameter like `service_id`, `routing` will be the
/// `service_id` associated with that batch. If the DLT does not take a
/// parameter, `routing` will always be `None`.
pub trait UrlResolver: Sync + Send {
    type Id: ScopeId;
    /// Generate an address (i.e. URL) to which the batch will be sent.
    fn url(&self, scope_id: &Self::Id) -> String;
}

/// An interface for interpretting and recording updates from the submitter
pub trait SubmitterObserver {
    type Id: ScopeId;
    /// Notify the observer of an update. The interpretation and recording
    /// of the update is determined by the observer's implementation.
    fn notify(
        &self,
        batch_header: String,
        scope_id: Self::Id,
        status: Option<u16>,
        message: Option<String>,
    );
}

pub trait SubmitterBuilder<
    S: ScopeId,
    R: UrlResolver<Id = S> + Sync + Send,
    Q: Iterator<Item = Submission<S>> + Send,
    O: SubmitterObserver<Id = S> + Send,
>
{
    type RunnableSubmitter: RunnableSubmitter<S, R, Q, O>;

    fn new() -> Self;

    fn with_url_resolver(&mut self, url_resolver: &'static R);

    fn with_queue(&mut self, queue: Q);

    fn with_observer(&mut self, observer: O);

    fn build(self) -> Result<Self::RunnableSubmitter, InternalError>;
}

pub trait RunnableSubmitter<
    S: ScopeId,
    R: UrlResolver<Id = S> + Sync + Send,
    Q: Iterator<Item = Submission<S>> + Send,
    O: SubmitterObserver<Id = S> + Send,
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

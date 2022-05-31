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

use crate::{batch_submission::Submission, error::InternalError, scope_id::ScopeId};

mod async_batch_submitter;
pub use async_batch_submitter::{
    BatchRunnableSubmitter, BatchRunningSubmitter, BatchSubmitterBuilder,
};

/// The interface for a submitter that is built but not yet running.
pub trait RunnableSubmitter<S: ScopeId, Q: Iterator<Item = Submission<S>> + Send> {
    type RunningSubmitter: RunningSubmitter;

    /// Start running the submission service.
    fn run(self) -> Result<Self::RunningSubmitter, InternalError>;
}

/// The interface for a running submitter service. This is effectively a handle to the service.
pub trait RunningSubmitter {
    /// Signal to the internal submitter components to begin the shutdown process.
    fn signal_shutdown(&self) -> Result<(), InternalError>;

    /// Wind down and stop the submission service.
    fn shutdown(self) -> Result<(), InternalError>;
}

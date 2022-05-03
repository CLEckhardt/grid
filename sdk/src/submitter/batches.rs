use super::{UrlResolver, ScopeId, ServiceScopeId, GlobalScopeId};

use crate::batch_tracking::store::{
    BatchStatus, SubmissionError, TrackingBatch, TrackingTransaction,
};


// TODO: store will use None for no service_id instead of const string after PRs

#[derive(Debug, PartialEq, Clone)]
pub struct Submission<S: ScopeId> {
    batch_header: String,
    scope_id: S,
    serialized_batch: Vec<u8>,
}

impl<S: ScopeId> Submission<S> {
    pub fn batch_header(&self) -> String {
        self.batch_header.to_string()
    }

    pub fn scope_id(&self) -> S {
        self.scope_id.clone()
    }

    pub fn serialized_batch(&self) -> Vec<u8> {
        self.serialized_batch.to_vec()
    }
}


#[derive(Debug, PartialEq, Clone)]
pub struct TrackingBatchServiceScopeId {
    scope_id: ServiceScopeId,
    batch_header: String,
    data_change_id: Option<String>,
    signer_public_key: String,
    trace: bool,
    serialized_batch: Vec<u8>,
    submitted: bool,
    created_at: i64,
    transactions: Vec<TrackingTransaction>,
    batch_status: Option<BatchStatus>,
    submission_error: Option<SubmissionError>,
}

impl TrackingBatchServiceScopeId {
    pub fn scope_id(&self) -> &ServiceScopeId {
        &self.scope_id
    }

    pub fn batch_header(&self) -> &str {
        &self.batch_header
    }

    pub fn serialized_batch(&self) -> &[u8] {
        &self.serialized_batch
    }

    pub fn data_change_id(&self) -> Option<&str> {
        self.data_change_id.as_deref()
    }

    pub fn signer_public_key(&self) -> &str {
        &self.signer_public_key
    }

    pub fn trace(&self) -> bool {
        self.trace
    }

    pub fn submitted(&self) -> bool {
        self.submitted
    }

    pub fn created_at(&self) -> i64 {
        self.created_at
    }

    pub fn transactions(&self) -> &[TrackingTransaction] {
        &self.transactions
    }

    pub fn batch_status(&self) -> Option<&BatchStatus> {
        self.batch_status.as_ref()
    }

    pub fn submission_error(&self) -> Option<&SubmissionError> {
        self.submission_error.as_ref()
    }

    pub fn into_submission<
        S: ScopeId,
        R: UrlResolver<Id = ServiceScopeId>,
    >(
        self,
        url_resolver: R,
    ) -> Submission<ServiceScopeId> {
        Submission {
            batch_header: self.batch_header,
            scope_id: self.scope_id,
            serialized_batch: self.serialized_batch,
        }
    }
}

// TODO: Implement this
//impl std::convert::TryFrom<TrackingBatch> for TrackingBatchServiceScopeId {}


#[derive(Debug, PartialEq, Clone)]
pub struct TrackingBatchGlobalScopeId {
    scope_id: GlobalScopeId,
    batch_header: String,
    data_change_id: Option<String>,
    signer_public_key: String,
    trace: bool,
    serialized_batch: Vec<u8>,
    submitted: bool,
    created_at: i64,
    transactions: Vec<TrackingTransaction>,
    batch_status: Option<BatchStatus>,
    submission_error: Option<SubmissionError>,
}

impl TrackingBatchGlobalScopeId {
    pub fn scope_id(&self) -> &GlobalScopeId {
        &self.scope_id
    }

    pub fn batch_header(&self) -> &str {
        &self.batch_header
    }

    pub fn serialized_batch(&self) -> &[u8] {
        &self.serialized_batch
    }

    pub fn data_change_id(&self) -> Option<&str> {
        self.data_change_id.as_deref()
    }

    pub fn signer_public_key(&self) -> &str {
        &self.signer_public_key
    }

    pub fn trace(&self) -> bool {
        self.trace
    }

    pub fn submitted(&self) -> bool {
        self.submitted
    }

    pub fn created_at(&self) -> i64 {
        self.created_at
    }

    pub fn transactions(&self) -> &[TrackingTransaction] {
        &self.transactions
    }

    pub fn batch_status(&self) -> Option<&BatchStatus> {
        self.batch_status.as_ref()
    }

    pub fn submission_error(&self) -> Option<&SubmissionError> {
        self.submission_error.as_ref()
    }

    pub fn into_submission<R: UrlResolver<Id = GlobalScopeId>>(
        self,
        url_resolver: R,
    ) -> Submission<GlobalScopeId> {
        Submission {
            batch_header: self.batch_header,
            scope_id: self.scope_id,
            serialized_batch: self.serialized_batch,
        }
    }
}

// TODO: Implement this
//impl std::convert::TryFrom<TrackingBatch> for TrackingBatchGlobalScopeId {}

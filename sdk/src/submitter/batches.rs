use super::WithServiceId;
use crate::batch_tracking::store::{BatchStatus, SubmissionError, TrackingTransaction};


pub trait VerifiedBatch {
    
    fn batch_header(&self) -> &str {
        &self.batch_header
    }
    fn serialized_batch(&self) -> &[u8] {
        &self.serialized_batch
    }
    // Marker trait for verified batches structs
}

#[derive(Debug, PartialEq, Clone)]
pub struct TrackingBatchWithSID {
    service_id: String,
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

impl TrackingBatchWithSID {
    pub fn service_id(&self) -> &str {
        &self.service_id
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
}

impl WithServiceId for TrackingBatchWithSID {}
impl VerifiedBatch for TrackingBatchWithSID {}

#[derive(Debug, PartialEq, Clone)]
pub struct TrackingBatchNoSID {
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

impl TrackingBatchNoSID {
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
}

impl VerifiedBatch for TrackingBatchNoSID {}

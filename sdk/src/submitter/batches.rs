use super::Addresser;
use crate::batch_tracking::store::{
    BatchStatus, SubmissionError, TrackingTransaction, NON_SPLINTER_SERVICE_ID_DEFAULT,
};
// TODO: store will use None for no service_id instead of const string after PRs

pub trait VerifiedBatch {
    // Marker trait for verified batches structs
}

#[derive(Debug, Clone, PartialEq)]
// Wraps the batch as it moves through the submitter
pub struct BatchEnvelope<T: TrackingId> {
    batch_id: Box<T>,
    address: String,
    serialized_batch: Vec<u8>,
}

impl<T: TrackingId> BatchEnvelope<T> {
    fn create(batch_id: Box<T>, address: String, serialized_batch: Vec<u8>) -> Self {
        Self {
            batch_id,
            address,
            serialized_batch,
        }
    }

    pub fn batch_id(&self) -> &T {
        &self.batch_id
    }
    pub fn address(&self) -> &String {
        &self.address
    }
    pub fn serialized_batch(&self) -> &Vec<u8> {
        &self.serialized_batch
    }
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
    //fn create<B: VerifiedBatch>(batch: &B) -> Self;

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

    /// Get the values of the `BatchTrackingId`, returned as a tuple.
    fn get(self) -> (String, String) {
        (self.batch_header, self.service_id)
    }
}

// TODO: Need to implement From TrackingBatch

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
    pub fn batch_header(&self) -> &str {
        &self.batch_header
    }

    pub fn service_id(&self) -> &str {
        &self.service_id
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

    pub fn into_batch_envelope<T: TrackingId, A: Addresser<Batch = TrackingBatchWithSID>>(
        self,
        addresser: A,
    ) -> BatchEnvelope<BatchTrackingId> {
        BatchEnvelope::create(
            Box::new(BatchTrackingId {
                batch_header: self.batch_header.clone(),
                service_id: self.service_id.clone(),
            }),
            addresser.address(&self),
            self.serialized_batch,
        )
    }
}

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

    pub fn into_batch_envelope<T: TrackingId, A: Addresser<Batch = TrackingBatchNoSID>>(
        self,
        addresser: A,
    ) -> BatchEnvelope<BatchTrackingId> {
        BatchEnvelope::create(
            Box::new(BatchTrackingId {
                batch_header: self.batch_header.clone(),
                service_id: NON_SPLINTER_SERVICE_ID_DEFAULT.to_string(),
            }),
            addresser.address(&self),
            self.serialized_batch,
        )
    }
}

impl VerifiedBatch for TrackingBatchNoSID {}

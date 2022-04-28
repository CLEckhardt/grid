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

//! Constructs the url where the batch will be sent.
//!

use super::{
    batches::{TrackingBatchNoSID, TrackingBatchWithSID},
    Addresser, /*BatchSubmission, SubmissionWithSID, TrackingId, */
};

#[derive(Debug, Clone, PartialEq)]
pub struct BatchAddresserWithSID {
    base_url: &'static str,
}

impl BatchAddresserWithSID {
    pub fn new(base_url: &'static str) -> Self {
        Self { base_url }
    }
}
/*
impl Addresser for BatchAddresserWithSID {
    fn address<T: TrackingId + WithServiceId>(&self, batch: SubmissionWithSID<T>) -> String {
        format!(
            "{base_url}?service_id={sid}",
            base_url = self.base_url,
            sid = batch.service_id()
        )
    }
}
*/

//impl<B: VerifiedBatch + WithServiceId> Addresser<B> for BatchAddresserWithSID {
impl Addresser for BatchAddresserWithSID {
    type Batch = TrackingBatchWithSID;
    fn address(&self, batch: TrackingBatchWithSID) -> String {
        format!(
            "{base_url}?service_id={sid}",
            base_url = self.base_url,
            sid = batch.service_id()
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BatchAddresserNoSID {
    base_url: &'static str,
}

impl BatchAddresserNoSID {
    /// Create a new addresser based on the requirements of the DLT. If the DLT
    /// does not take a URL parameter like `service-id`, the parameter is
    /// `None`.
    pub fn new(base_url: &'static str) -> Self {
        Self { base_url }
    }
}

impl Addresser for BatchAddresserNoSID {
    type Batch = TrackingBatchNoSID;
    /// Generate the URL to which the batch should be sent.
    fn address(&self, batch: TrackingBatchNoSID) -> String {
        // Batch info isn't used when there is no service_id
        let _ = batch;
        self.base_url.to_string()
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_batch_submitter_batch_addresser_with_sid_new() {
        let expected = BatchAddresserWithSID {
            base_url: "test.com",
        };

        assert_eq!(BatchAddresserWithSID::new("test.com"), expected);
    }

    #[test]
    fn test_batch_submitter_batch_addresser_no_sid_new() {
        let expected = BatchAddresserNoSID {
            base_url: "test.com",
        };

        assert_eq!(BatchAddresserNoSID::new("test.com"), expected);
    }

    /*
    #[test]
    fn test_batch_submitter_batch_addresser_new() {
        let expected_addresser_wo_serv = BatchAddresserWithSID {
            base_url: "http://127.0.0.1:8080",
        };
        let expected_addresser_w_serv = BatchAddresserWithSID {
            base_url: "http://127.0.0.1:8080",
        };

        assert_eq!(
            BatchAddresserWithSID::new("http://127.0.0.1:8080"),
            expected_addresser_wo_serv
        );
        assert_eq!(
            BatchAddresserWithSID::new("http://127.0.0.1:8080"),
            expected_addresser_w_serv
        );
    }

    #[test]
    fn test_batch_submitter_batch_addresser() {
        let test_addresser_wo_serv = BatchAddresserWithSID::new("http://127.0.0.1:8080", None);
        let test_addresser_w_serv =
            BatchAddresserWithSID::new("http://127.0.0.1:8080", Some("service_id"));

        assert_eq!(
            test_addresser_wo_serv.address(None).unwrap(),
            "http://127.0.0.1:8080".to_string()
        );
        assert_eq!(
            test_addresser_w_serv
                .address(Some("123-abc".to_string()))
                .unwrap(),
            "http://127.0.0.1:8080?service_id=123-abc".to_string()
        );
        assert!(test_addresser_wo_serv
            .address(Some("123-abc".to_string()))
            .is_err());
        assert!(test_addresser_w_serv.address(None).is_err());
    }*/
}

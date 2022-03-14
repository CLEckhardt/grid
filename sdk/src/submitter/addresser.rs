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
//! Generically, the addresser generates an address to which a batch will be
//! sent, possibly including specific routing information. In this case, the
//! addresser contains information about the DLT, namely, the REST endpoint to
//! which batches can be submitted and the url parameter it accepts, if
//! applicable.

use super::Addresser;
use crate::error::InternalError;

#[derive(Debug, Clone, PartialEq)]
pub struct BatchAddresser {
    base_url: &'static str,
    parameter: Option<&'static str>,
}

impl BatchAddresser {
    /// Create a new addresser based on the requirements of the DLT. If the DLT
    /// does not take a URL parameter like `service-id`, the parameter is
    /// `None`.
    pub fn new(base_url: &'static str, parameter: Option<&'static str>) -> Self {
        Self {
            base_url,
            parameter,
        }
    }
}

impl Addresser for BatchAddresser {
    /// Generate the URL to which the batch should be sent.
    fn address(&self, routing: Option<String>) -> Result<String, InternalError> {
        match &self.parameter {
            Some(p) => {
                if let Some(r) = routing {
                    Ok(format!(
                        "{base_url}?{parameter}={route}",
                        base_url = self.base_url,
                        parameter = p,
                        route = r,
                    ))
                } else {
                    Err(InternalError::with_message(
                        "Addressing error: expecting service_id for batch but none was provided"
                            .to_string(),
                    ))
                }
            }
            None => {
                if routing.is_none() {
                    Ok(self.base_url.to_string())
                } else {
                    Err(InternalError::with_message(
                        "Addressing error: service_id for batch was provided but none was expected"
                            .to_string(),
                    ))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_batch_submitter_batch_addresser_new() {
        let expected_addresser_wo_serv = BatchAddresser {
            base_url: "http://127.0.0.1:8080",
            parameter: None,
        };
        let expected_addresser_w_serv = BatchAddresser {
            base_url: "http://127.0.0.1:8080",
            parameter: Some("service_id"),
        };

        assert_eq!(
            BatchAddresser::new("http://127.0.0.1:8080", None),
            expected_addresser_wo_serv
        );
        assert_eq!(
            BatchAddresser::new("http://127.0.0.1:8080", Some("service_id")),
            expected_addresser_w_serv
        );
    }

    #[test]
    fn test_batch_submitter_batch_addresser() {
        let test_addresser_wo_serv = BatchAddresser::new("http://127.0.0.1:8080", None);
        let test_addresser_w_serv =
            BatchAddresser::new("http://127.0.0.1:8080", Some("service_id"));

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
    }
}

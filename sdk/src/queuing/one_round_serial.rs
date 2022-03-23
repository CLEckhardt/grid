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

// NOTE: There is no queue trait, since it is an Iterator

// TODO: Update StoreAbstraction methods to match the actual store

use crate::{
    batch_tracking::store::{BatchStatus, TrackingBatch},
    error::InternalError,
    submitter::BatchSubmission,
};

const RESUBMISSION_DELAY: std::time::Duration = std::time::Duration::new(10, 0);


// For testing/dev only
//#[derive(Debug)]//, Eq, PartialEq)]
pub struct StoreAbstraction {
    batches: Vec<TrackingBatch>,
}

// TODO: Replace this with the store trait

impl StoreAbstraction {
    fn mock_new(mock_batches: Vec<TrackingBatch>) -> Self {
        Self {
            batches: mock_batches,
        }
    }

    fn mock_empty() -> Self {
        Self {
            batches: Vec::new(),
        }
    }

    fn get_batches(&mut self) -> Vec<TrackingBatch> {
        self.batches.clone()
    }
}

// -----------------------------

//#[derive(Debug)]//, Eq, PartialEq)]
pub struct BatchQueueStrategyOneRoundSerial {
    store: StoreAbstraction,
    queue: Vec<BatchSubmission>,
}

impl Iterator for BatchQueueStrategyOneRoundSerial {
    type Item = BatchSubmission;

    fn next(&mut self) -> Option<BatchSubmission> {
        let next = self.queue.pop();
        match next {
            Some(b) => Some(b),
            None => {
                self.replenish_queue();
                // Returns None if there are no batches to queue
                self.queue.pop()
            }
        }
    }
}

impl BatchQueueStrategyOneRoundSerial {
    fn new(store: StoreAbstraction) -> Self {
        Self {
            store,
            queue: Vec::new(),
        }
    }

    fn replenish_queue(&mut self) {
        self.queue = Self::run_strategy(&mut self.store);
    }

    fn find_next_batch(
        service_id: &String,
        new_batches: &Vec<&TrackingBatch>,
    ) -> Result<String, InternalError> {
        // Algorithm for efficiently finding the next batch
        // Avoids implementing ordering traits for many structs
        let mut current_batch_header: Option<String> = None;
        let mut current_created_at: Option<i64> = None;

        for b in new_batches {
            if b.service_id() != service_id {
                continue;
            };

            if current_batch_header.is_none() {
                current_batch_header = Some(b.batch_header().to_string());
                current_created_at = Some(b.created_at());
            } else {
                if Some(b.created_at()) < current_created_at {
                    current_batch_header = Some(b.batch_header().to_string());
                    current_created_at = Some(b.created_at());
                } else if Some(b.created_at()) == current_created_at {
                    if Some(b.batch_header().to_string()) < current_batch_header {
                        current_batch_header = Some(b.batch_header().to_string());
                        current_created_at = Some(b.created_at());
                    }
                }
            }
        }

        match current_batch_header {
            Some(b) => Ok(b),
            None => Err(InternalError::with_message(
                "Error finding next batch. This really \
            shouldn't happen"
                    .to_string(),
            )),
        }
    }

    fn run_strategy(store: &mut StoreAbstraction) -> Vec<BatchSubmission> {
        let batch_candidates = store.get_batches();

        // Get all batches with Unknown status and Delayed status past the resubmission delay
        // We know we want to submit these
        let mut batch_queue = batch_candidates
            .iter()
            .filter(|b| {
                b.batch_status()
                    == Some(&BatchStatus::Unknown)
                        | (b.batch_status() == Some(&BatchStatus::Delayed)
                            && std::time::Instant::now()
                                .duration_since(store.get_last_updated_time(b.batch_header()))
                                < RESUBMISSION_DELAY)
            })
            .map(|b| {
                BatchSubmission::new(
                    b.batch_header().to_string(),
                    Some(b.service_id().to_string()),
                    b.serialized_batch().to_vec(),
                )
            })
            .collect();

        // Exclude all service_ids where there is a batch with Pending, Unknown, or Delayed status
        // Batches with those statuses must be submitted first
        let mut excluded_service_ids = batch_candidates
            .iter()
            .filter(|b| {
                b.batch_status() == Some(&BatchStatus::Pending)
                    || b.batch_status() == Some(&BatchStatus::Unknown)
                    || b.batch_status() == Some(&BatchStatus::Delayed)
            })
            .map(|b| b.service_id().to_string())
            .collect::<Vec<String>>();
        excluded_service_ids.sort_unstable();
        excluded_service_ids.dedup();

        // Get service_ids, filtering out the excluded ones
        let mut service_ids = batch_candidates
            .iter()
            .map(|b| b.service_id().to_string())
            .filter(|b| !excluded_service_ids.iter().any(|s| s == b))
            .collect::<Vec<String>>();
        service_ids.sort_unstable();
        service_ids.dedup();

        // Get batches of above service_ids
        let new_batches = batch_candidates
            .iter()
            .filter(|b| service_ids.iter().any(|s| s == b.service_id()))
            .collect::<Vec<&TrackingBatch>>();

        // Get the next batch for each service_id identified above
        // These are the "new" batches that need to be submitted
        for id in service_ids {
            if let Ok(next_batch_header) = Self::find_next_batch(&id, &new_batches) {
                if let Some(next_batch) = new_batches
                    .iter()
                    .find(|b| b.batch_header() == next_batch_header)
                {
                    batch_queue.push(next_batch);
                }
            }
        }

        batch_queue
    }
}

// TESTS ----------------------

#[cfg(test)]
mod tests {

/*    use crate::{
        BatchInfo, BatchQueueStrategy, BatchQueueStrategyOneRoundSerial, BatchStatus,
        BatchSubmission, StoreAbstraction,
    };*/

    use super::*;

    struct MockBatches {
        batches: Vec<TrackingBatch>,
    }
    impl MockBatches {
        fn new_set() -> MockBatches {
            MockBatches {
                batches: vec![
                    TrackingBatch {
                        service_id: "abcd-1234".to_string(),
                        batch_header: "b1_1234567890abcdef".to_string(),
                        data_change_id: None,
                        signer_public_key: "0".to_string(),
                        trace: false,
                        serialized_batch: vec![1, 1, 1, 1],
                        submitted: false,
                        created_at: 10001,
                        transactions: Vec::new(),
                        batch_status: None,
                        submission_error: None,
                    },
                    TrackingBatch {
                        service_id: "abcd-1234".to_string(),
                        batch_header: "b2_1234567890abcdef".to_string(),
                        data_change_id: None,
                        signer_public_key: "0".to_string(),
                        trace: false,
                        serialized_batch: vec![2, 2, 2, 2],
                        submitted: false,
                        created_at: 10002,
                        transactions: Vec::new(),
                        batch_status: None,
                        submission_error: None,
                    },
                    TrackingBatch {
                        service_id: "efgh-5678".to_string(),
                        batch_header: "b3_1234567890abcdef".to_string(),
                        data_change_id: None,
                        signer_public_key: "0".to_string(),
                        trace: false,
                        serialized_batch: vec![3, 3, 3, 3],
                        submitted: false,
                        created_at: 10003,
                        transactions: Vec::new(),
                        batch_status: None,
                        submission_error: None,
                    },
                    TrackingBatch {
                        service_id: "efgh-5678".to_string(),
                        batch_header: "b4_1234567890abcdef".to_string(),
                        data_change_id: None,
                        signer_public_key: "0".to_string(),
                        trace: false,
                        serialized_batch: vec![4, 4, 4, 4],
                        submitted: false,
                        created_at: 10004,
                        transactions: Vec::new(),
                        batch_status: None,
                        submission_error: None,
                    },
                    TrackingBatch {
                        service_id: "abcd-1234".to_string(),
                        batch_header: "b5_1234567890abcdef".to_string(),
                        data_change_id: None,
                        signer_public_key: "0".to_string(),
                        trace: false,
                        serialized_batch: vec![5, 5, 5, 5],
                        submitted: false,
                        created_at: 10005,
                        transactions: Vec::new(),
                        batch_status: None,
                        submission_error: None,
                    },
                    TrackingBatch {
                        service_id: "ayay-1212".to_string(),
                        batch_header: "b6_1234567890abcdef".to_string(),
                        data_change_id: None,
                        signer_public_key: "0".to_string(),
                        trace: false,
                        serialized_batch: vec![6, 6, 6, 6],
                        submitted: false,
                        created_at: 10006,
                        transactions: Vec::new(),
                        batch_status: None,
                        submission_error: None,
                    },
                    TrackingBatch {
                        service_id: "abcd-1234".to_string(),
                        batch_header: "b7_1234567890abcdef".to_string(),
                        data_change_id: None,
                        signer_public_key: "0".to_string(),
                        trace: false,
                        serialized_batch: vec![7, 7, 7, 7],
                        submitted: false,
                        created_at: 10007,
                        transactions: Vec::new(),
                        batch_status: None,
                        submission_error: None,
                    },
                    TrackingBatch {
                        service_id: "ayay-1212".to_string(),
                        batch_header: "b8_1234567890abcdef".to_string(),
                        data_change_id: None,
                        signer_public_key: "0".to_string(),
                        trace: false,
                        serialized_batch: vec![8, 8, 8, 8],
                        submitted: false,
                        created_at: 10008,
                        transactions: Vec::new(),
                        batch_status: None,
                        submission_error: None,
                    },
                    TrackingBatch {
                        service_id: "ayay-1212".to_string(),
                        batch_header: "b9_1234567890abcdef".to_string(),
                        data_change_id: None,
                        signer_public_key: "0".to_string(),
                        trace: false,
                        serialized_batch: vec![9, 9, 9, 9],
                        submitted: false,
                        created_at: 10009,
                        transactions: Vec::new(),
                        batch_status: None,
                        submission_error: None,
                    },
                ],
            }
        }

        fn new_set_w_pending() -> MockBatches {
            let mut mock_batches = MockBatches::new_set();
            mock_batches.batches.push(TrackingBatch {
                service_id: "abcd-1234".to_string(),
                batch_header: "b0_1234567890abcdef".to_string(),
                data_change_id: None,
                signer_public_key: "0".to_string(),
                trace: false,
                serialized_batch: vec![0, 0, 0, 0],
                submitted: false,
                created_at: 10000,
                transactions: Vec::new(),
                batch_status: Some(BatchStatus::Pending),
                submission_error: None,
            });
            mock_batches
        }

        fn new_set_w_fast_batch() -> MockBatches {
            // Creates a batch that was submitted at the same time as batch 1
            // This batch should be submitted first
            let mut mock_batches = MockBatches::new_set();
            mock_batches.batches.push(TrackingBatch {
                service_id: "abcd-1234".to_string(),
                batch_header: "b0_1234567890abcdef".to_string(),
                data_change_id: None,
                signer_public_key: "0".to_string(),
                trace: false,
                serialized_batch: vec![0, 0, 0, 0],
                submitted: false,
                created_at: 10001,
                transactions: Vec::new(),
                batch_status: Some(BatchStatus::Unknown),
                submission_error: None,
            });
            mock_batches
        }
    }

    #[test]
    fn test_batch_queue_strategy_one_round_serial_next_batch_full() {
        let mut test_queue = BatchQueueStrategyOneRoundSerial {
            store: StoreAbstraction::mock_empty(),
            queue: vec![BatchSubmission {
                id: "abcd".to_string(),
                service_id: Some("abcd-1234".to_string()),
                serialized_batch: vec![1, 1, 1, 1],
            }],
        };
        assert_eq!(
            test_queue.next(),
            Some(BatchSubmission {
                id: "b1_1234567890abcdef".to_string(),
                service_id: Some("abcd-1234".to_string()),
                serialized_batch: vec![1, 1, 1, 1],
            })
        )
    }

    #[test]
    fn test_batch_queue_strategy_one_round_serial_next_batch_empty_wo_replen() {
        let mut test_queue = BatchQueueStrategyOneRoundSerial {
            store: StoreAbstraction::mock_empty(),
            queue: Vec::<BatchSubmission>::new(),
        };
        assert_eq!(test_queue.next(), None)
    }

    #[test]
    fn test_batch_queue_strategy_one_round_serial_next_batch_empty_w_replen() {
        let mock_batches = vec![TrackingBatch {
            service_id: "abcd-1234".to_string(),
            batch_header: "b1_1234567890abcdef".to_string(),
            data_change_id: None,
            signer_public_key: "0".to_string(),
            trace: false,
            serialized_batch: vec![1, 1, 1, 1],
            submitted: false,
            created_at: 10001,
            transactions: Vec::new(),
            batch_status: None,
            submission_error: None,
        }];
        let mut test_queue = BatchQueueStrategyOneRoundSerial {
            store: StoreAbstraction::mock_new(mock_batches),
            queue: Vec::<BatchSubmission>::new(),
        };
        // The queue starts empty, so next_batch should cause the queue to replenish
        assert_eq!(
            test_queue.next(),
            Some(BatchSubmission {
                id: "b1_1234567890abcdef".to_string(),
                service_id: Some("abcd-1234".to_string()),
                serialized_batch: vec![1, 1, 1, 1],
            })
        )
    }

    #[test]
    fn test_batch_queue_strategy_one_round_serial_next_batch_empty_w_replen_empty() {
        let mut test_queue = BatchQueueStrategyOneRoundSerial {
            store: StoreAbstraction::mock_empty(),
            queue: Vec::<BatchSubmission>::new(),
        };
        // The queue starts empty, so next_batch should cause the queue to replenish
        // In this case, there are no batches to queue, so next_batch returns empty
        assert_eq!(test_queue.next(), None)
    }

    #[test]
    fn test_batch_queue_strategy_get_service_ids() {
        let mock_batches = MockBatches::new_set();

        assert_eq!(
            BatchQueueStrategyOneRoundSerial::get_service_ids(&mock_batches.batches),
            vec![
                "abcd-1234".to_string(),
                "ayay-1212".to_string(),
                "efgh-5678".to_string(),
            ]
        )
    }

    #[test]
    fn test_batch_queue_strategy_one_round_serial_run_strategy() {
        let mock_batches = MockBatches::new_set();

        let test_queue = BatchQueueStrategyOneRoundSerial {
            store: StoreAbstraction::mock_new(mock_batches.batches),
            queue: Vec::<BatchSubmission>::new(),
        };

        assert_eq!(
            BatchQueueStrategyOneRoundSerial::run_strategy(&mut test_queue.store),
            vec![
                BatchSubmission {
                    id: "b1_1234567890abcdef".to_string(),
                    service_id: Some("abcd-1234".to_string()),
                    serialized_batch: vec![1, 1, 1, 1],
                },
                BatchSubmission {
                    id: "b6_1234567890abcdef".to_string(),
                    service_id: Some("ayay-1212".to_string()),
                    serialized_batch: vec![6, 6, 6, 6],
                },
                BatchSubmission {
                    id: "b3_1234567890abcdef".to_string(),
                    service_id: Some("efgh-5678".to_string()),
                    serialized_batch: vec![3, 3, 3, 3],
                },
            ]
        )
    }

    #[test]
    fn test_batch_queue_strategy_one_round_serial_run_strategy_w_pending_batch() {
        let mock_batches = MockBatches::new_set_w_pending();

        let test_queue = BatchQueueStrategyOneRoundSerial {
            store: StoreAbstraction::mock_new(mock_batches.batches),
            queue: Vec::<BatchSubmission>::new(),
        };

        assert_eq!(
            BatchQueueStrategyOneRoundSerial::run_strategy(&mut test_queue.store),
            vec![
                BatchSubmission {
                    id: "b6_1234567890abcdef".to_string(),
                    service_id: Some("ayay-1212".to_string()),
                    serialized_batch: vec![6, 6, 6, 6],
                },
                BatchSubmission {
                    id: "b3_1234567890abcdef".to_string(),
                    service_id: Some("efgh-5678".to_string()),
                    serialized_batch: vec![3, 3, 3, 3],
                },
            ]
        )
    }

    #[test]
    fn test_batch_queue_strategy_one_round_serial_run_strategy_w_fast_batch() {
        let mock_batches = MockBatches::new_set_w_fast_batch();

        let test_queue = BatchQueueStrategyOneRoundSerial {
            store: StoreAbstraction::mock_new(mock_batches.batches),
            queue: Vec::<BatchSubmission>::new(),
        };

        assert_eq!(
            BatchQueueStrategyOneRoundSerial::run_strategy(&mut test_queue.store),
            vec![
                BatchSubmission {
                    id: "b0_1234567890abcdef".to_string(),
                    service_id: Some("abcd-1234".to_string()),
                    serialized_batch: vec![0, 0, 0, 0],
                },
                BatchSubmission {
                    id: "b6_1234567890abcdef".to_string(),
                    service_id: Some("ayay-1212".to_string()),
                    serialized_batch: vec![6, 6, 6, 6],
                },
                BatchSubmission {
                    id: "b3_1234567890abcdef".to_string(),
                    service_id: Some("efgh-5678".to_string()),
                    serialized_batch: vec![3, 3, 3, 3],
                },
            ]
        )
    }
}

// NOTE: There is no queue trait, since it is an Iterator

// TODO: Pull the BatchQueueStrategy trait out to mod
// TODO: Update StoreAbstraction methods to match the actual store

// Abstraction for a batch submission
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BatchSubmission {
    id: String,
    service_id: String,
    serialized_batch: Vec<u8>,
}

// This is the per-batch struct we get from the store
// TODO: Determine what struct will come from the store
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BatchInfo {
    service_id: String,
    submitted: bool,
    header_signature: String,
    created_at: i64,
    batch_status: Option<BatchStatus>,
    batch: String, // placeholder
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[allow(dead_code)]
enum BatchStatus {
    Pending,
    Invalid,
    Valid,
    Committed,
    Unknown,
}

// For testing/dev only
#[derive(Debug, Eq, PartialEq)]
pub struct StoreAbstraction {
    batches: Vec<BatchInfo>,
}

impl StoreAbstraction {
    fn mock_new(mock_batches: Vec<BatchInfo>) -> StoreAbstraction {
        StoreAbstraction {
            batches: mock_batches,
        }
    }

    fn mock_empty() -> StoreAbstraction {
        StoreAbstraction {
            batches: Vec::new(),
        }
    }

    fn get_batches(&self) -> Vec<BatchInfo> {
        self.batches.clone()
    }
}

// -----------------------------

#[derive(Debug, Eq, PartialEq)]
pub struct BatchQueueStrategyOneRoundSerial {
    store: StoreAbstraction,
    queue: Vec<BatchSubmission>,
}

impl Iterator<Item = BatchSubmission> for BatchQueueStrategyOneRoundSerial {
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
    fn new(store: StoreAbstraction) -> BatchQueueStrategyOneRoundSerial {
        BatchQueueStrategyOneRoundSerial {
            store: store,
            queue: Vec::new(),
        }
    }

    fn replenish_queue(&mut self) {
        self.queue = Self::run_strategy(&self.store);
    }

    fn run_strategy(store: &StoreAbstraction) -> Vec<BatchSubmission> {
        let batch_candidates = store.get_batches();
        let mut batch_queue: Vec<BatchInfo> = Vec::new();

        // Get a list of the service_ids
        let services = Self::get_service_ids(&batch_candidates);

        // For each service id
        for id in services {
            if !batch_candidates
                .iter()
                .any(|b| b.service_id == id && b.batch_status == Some(BatchStatus::Pending))
            {
                let service_queue = batch_candidates
                    .iter()
                    .filter(|b| b.service_id == id)
                    .collect::<Vec<&BatchInfo>>();
                // Finds the oldest created_at, then the batch with that timestamp
                // This avoids implementing PartialOrd and Ord on many structs

// TODO: WORKING HERE

                if let Some(oldest_batch_created_at) =
                    service_queue.iter().map(|b| b.created_at).min()
                {
                    let mut next_batch = service_queue
                        .iter()
                        .filter(|b| b.created_at == oldest_batch_created_at)
                        .map(|b| *b)
                        .collect::<Vec<&BatchInfo>>();
                    if next_batch.len() == 1 {
                        if let Some(batch) = next_batch.pop() {
                            batch_queue.push(batch.clone());
                        }
                    } else if next_batch.len() > 1 {
                        if let Some(first_batch) =
                            next_batch.iter().map(|b| b.header_signature.clone()).min()
                        {
                            match next_batch
                                .iter()
                                .find(|b| b.header_signature == first_batch)
                            {
                                Some(b) => batch_queue.push(batch.clone()),
                                None => {} // TODO: Log this error!
                            }
                        }
                    }
                }

                /*
                if service_queue.len() > 0 {
                    let oldest_batch_created_at =
                        // TODO Do something about this unwrap
                        service_queue.iter().map(|b| b.created_at).min().unwrap();
                    let mut next_batch = service_queue
                        .iter()
                        .filter(|b| b.created_at == oldest_batch_created_at)
                        .map(|b| *b)
                        .collect::<Vec<&BatchInfo>>();
                    if next_batch.len() == 1 {
                        if let Some(batch) = next_batch.pop() {
                            batch_queue.push(batch.clone());
                        }
                    }
                    // If more than one batch has the oldest timestamp, sort by header_signature
                    // and select the first one
                    // This ensures that batch submission behavior is deterministic
                    if next_batch.len() > 1 {
                        // TODO Do something about this unwrap
                        let first_batch = next_batch
                            .iter()
                            .map(|b| b.header_signature.clone())
                            .min()
                            .unwrap();
                        let batch_to_queue = next_batch
                            .iter()
                            .find(|b| b.header_signature == first_batch);
                        if let Some(&batch) = batch_to_queue {
                            batch_queue.push(batch.clone());
                        }
                    }
                }*/
            }
        }

        batch_queue
            .iter()
            .map(|b| BatchSubmission {
                service_id: b.service_id.clone(),
                abstraction: b.batch.clone(),
            })
            .collect::<Vec<BatchSubmission>>()
    }
}

// TESTS ----------------------

#[cfg(test)]
mod tests {

    use crate::{
        BatchInfo, BatchQueueStrategy, BatchQueueStrategyOneRoundSerial, BatchStatus,
        BatchSubmission, StoreAbstraction,
    };

    struct MockBatches {
        batches: Vec<BatchInfo>,
    }
    impl MockBatches {
        fn new_set() -> MockBatches {
            MockBatches {
                batches: vec![
                    BatchInfo {
                        service_id: "abcd-1234".to_string(),
                        submitted: false,
                        header_signature: "b1_1234567890abcdef".to_string(),
                        created_at: 10001,
                        batch_status: None,
                        batch: "Here's test batch 1".to_string(),
                    },
                    BatchInfo {
                        service_id: "abcd-1234".to_string(),
                        submitted: false,
                        header_signature: "b2_1234567890abcdef".to_string(),
                        created_at: 10002,
                        batch_status: None,
                        batch: "Here's test batch 2".to_string(),
                    },
                    BatchInfo {
                        service_id: "efgh-5678".to_string(),
                        submitted: false,
                        header_signature: "b3_1234567890abcdef".to_string(),
                        created_at: 10003,
                        batch_status: None,
                        batch: "Here's test batch 3".to_string(),
                    },
                    BatchInfo {
                        service_id: "efgh-5678".to_string(),
                        submitted: false,
                        header_signature: "b4_1234567890abcdef".to_string(),
                        created_at: 10004,
                        batch_status: None,
                        batch: "Here's test batch 4".to_string(),
                    },
                    BatchInfo {
                        service_id: "abcd-1234".to_string(),
                        submitted: false,
                        header_signature: "b5_1234567890abcdef".to_string(),
                        created_at: 10005,
                        batch_status: None,
                        batch: "Here's test batch 5".to_string(),
                    },
                    BatchInfo {
                        service_id: "ayay-1212".to_string(),
                        submitted: false,
                        header_signature: "b6_1234567890abcdef".to_string(),
                        created_at: 10006,
                        batch_status: None,
                        batch: "Here's test batch 6".to_string(),
                    },
                    BatchInfo {
                        service_id: "abcd-1234".to_string(),
                        submitted: false,
                        header_signature: "b7_1234567890abcdef".to_string(),
                        created_at: 10007,
                        batch_status: None,
                        batch: "Here's test batch 7".to_string(),
                    },
                    BatchInfo {
                        service_id: "ayay-1212".to_string(),
                        submitted: false,
                        header_signature: "b8_1234567890abcdef".to_string(),
                        created_at: 10008,
                        batch_status: None,
                        batch: "Here's test batch 8".to_string(),
                    },
                    BatchInfo {
                        service_id: "ayay-1212".to_string(),
                        submitted: false,
                        header_signature: "b9_1234567890abcdef".to_string(),
                        created_at: 10009,
                        batch_status: None,
                        batch: "Here's test batch 9".to_string(),
                    },
                ],
            }
        }

        fn new_set_w_pending() -> MockBatches {
            let mut mock_batches = MockBatches::new_set();
            mock_batches.batches.push(BatchInfo {
                service_id: "abcd-1234".to_string(),
                submitted: true,
                header_signature: "b0_1234567890abcdef".to_string(),
                created_at: 10000,
                batch_status: Some(BatchStatus::Pending),
                batch: "Here's test batch 0".to_string(),
            });
            mock_batches
        }

        fn new_set_w_fast_batch() -> MockBatches {
            // Creates a batch that was submitted at the same time as batch 1
            // This batch should be submitted first
            let mut mock_batches = MockBatches::new_set();
            mock_batches.batches.push(BatchInfo {
                service_id: "abcd-1234".to_string(),
                submitted: false,
                header_signature: "b0_1234567890abcdef".to_string(),
                created_at: 10001,
                batch_status: Some(BatchStatus::Unknown),
                batch: "Here's test batch 0".to_string(),
            });
            mock_batches
        }
    }

    #[test]
    fn test_batch_queue_strategy_one_round_serial_new() {
        let mock_store = StoreAbstraction::mock_empty();
        let new_queue = BatchQueueStrategyOneRoundSerial::new(mock_store);
        assert_eq!(
            new_queue,
            BatchQueueStrategyOneRoundSerial {
                store: StoreAbstraction::mock_empty(),
                queue: Vec::<BatchSubmission>::new(),
            }
        );
    }

    #[test]
    fn test_batch_queue_strategy_one_round_serial_next_batch_full() {
        let mut test_queue = BatchQueueStrategyOneRoundSerial {
            store: StoreAbstraction::mock_empty(),
            queue: vec![BatchSubmission {
                service_id: "abcd-1234".to_string(),
                abstraction: "Here's a test batch".to_string(),
            }],
        };
        assert_eq!(
            test_queue.next_batch(),
            Some(BatchSubmission {
                service_id: "abcd-1234".to_string(),
                abstraction: "Here's a test batch".to_string()
            })
        )
    }

    #[test]
    fn test_batch_queue_strategy_one_round_serial_next_batch_empty_wo_replen() {
        let mut test_queue = BatchQueueStrategyOneRoundSerial {
            store: StoreAbstraction::mock_empty(),
            queue: Vec::<BatchSubmission>::new(),
        };
        assert_eq!(test_queue.next_batch(), None)
    }

    #[test]
    fn test_batch_queue_strategy_one_round_serial_next_batch_empty_w_replen() {
        let mock_batches = vec![BatchInfo {
            service_id: "abcd-1234".to_string(),
            submitted: false,
            header_signature: "1234567890abcdef".to_string(),
            created_at: 10001,
            batch_status: None,
            batch: "Here's a test batch".to_string(),
        }];
        let mut test_queue = BatchQueueStrategyOneRoundSerial {
            store: StoreAbstraction::mock_new(mock_batches),
            queue: Vec::<BatchSubmission>::new(),
        };
        // The queue starts empty, so next_batch should cause the queue to replenish
        assert_eq!(
            test_queue.next_batch(),
            Some(BatchSubmission {
                service_id: "abcd-1234".to_string(),
                abstraction: "Here's a test batch".to_string()
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
        assert_eq!(test_queue.next_batch(), None)
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
            BatchQueueStrategyOneRoundSerial::run_strategy(&test_queue.store),
            vec![
                BatchSubmission {
                    service_id: "abcd-1234".to_string(),
                    abstraction: "Here's test batch 1".to_string()
                },
                BatchSubmission {
                    service_id: "ayay-1212".to_string(),
                    abstraction: "Here's test batch 6".to_string()
                },
                BatchSubmission {
                    service_id: "efgh-5678".to_string(),
                    abstraction: "Here's test batch 3".to_string()
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
            BatchQueueStrategyOneRoundSerial::run_strategy(&test_queue.store),
            vec![
                BatchSubmission {
                    service_id: "ayay-1212".to_string(),
                    abstraction: "Here's test batch 6".to_string()
                },
                BatchSubmission {
                    service_id: "efgh-5678".to_string(),
                    abstraction: "Here's test batch 3".to_string()
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
            BatchQueueStrategyOneRoundSerial::run_strategy(&test_queue.store),
            vec![
                BatchSubmission {
                    service_id: "abcd-1234".to_string(),
                    abstraction: "Here's test batch 0".to_string()
                },
                BatchSubmission {
                    service_id: "ayay-1212".to_string(),
                    abstraction: "Here's test batch 6".to_string()
                },
                BatchSubmission {
                    service_id: "efgh-5678".to_string(),
                    abstraction: "Here's test batch 3".to_string()
                },
            ]
        )
    }
}

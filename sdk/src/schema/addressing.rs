// Copyright 2018-2021 Cargill Incorporated
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

use crypto::digest::Digest;
use crypto::sha2::Sha512;

pub const GRID_NAMESPACE: &str = "621dee";
pub const SCHEMA_PREFIX: &str = "01";
pub const GRID_SCHEMA_NAMESPACE: &str = "621dee01";

/// Computes the address a Grid Schema is stored at based on its name
pub fn compute_schema_address(name: &str) -> String {
    let mut sha = Sha512::new();
    sha.input(name.as_bytes());
    // (Grid namespace) + (schema namespace) + hash
    let hash_str = String::from(GRID_NAMESPACE) + SCHEMA_PREFIX + &sha.result_str();
    hash_str[..70].to_string()
}

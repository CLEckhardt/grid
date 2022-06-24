// Copyright 2018-2022 Cargill Incorporated
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

use crate::config::error::GriddleConfigError;
use crate::config::{GriddleConfig, PartialGriddleConfig};

pub trait PartialGriddleConfigBuilder {
    fn build(self) -> Result<PartialGriddleConfig, GriddleConfigError>;
}

pub struct GriddleConfigBuilder {
    partial_configs: Vec<PartialGriddleConfig>,
}

impl GriddleConfigBuilder {
    pub fn new() -> Self {
        GriddleConfigBuilder {
            partial_configs: Vec::new(),
        }
    }

    /// Adds a `PartialGriddleConfig` to the `GriddleConfigBuilder` object
    ///
    /// # Arguments
    ///
    /// * `partial` - A `PartialGriddleConfig` object generated by a configuration module.
    ///
    pub fn with_partial_config(mut self, partial: PartialGriddleConfig) -> Self {
        self.partial_configs.push(partial);
        self
    }

    /// Build a `GriddleConfig` object by incorporating values from each `partial_config`
    pub fn build(self) -> Result<GriddleConfig, GriddleConfigError> {
        let signing_key = self
            .partial_configs
            .iter()
            .find_map(|p| p.signing_key().map(|v| (v, p.source())))
            .ok_or_else(|| GriddleConfigError::MissingValue("signing key".to_string()))?;

        let rest_api_endpoint = self
            .partial_configs
            .iter()
            .find_map(|p| p.rest_api_endpoint().map(|v| (v, p.source())))
            .ok_or_else(|| GriddleConfigError::MissingValue("rest api endpoint".to_string()))?;
        #[cfg(feature = "proxy")]
        let proxy_forward_url = self
            .partial_configs
            .iter()
            .find_map(|p| p.proxy_forward_url().map(|v| (v, p.source())))
            .ok_or_else(|| GriddleConfigError::MissingValue("proxy forward url".to_string()))?;

        let scope = self
            .partial_configs
            .iter()
            .find_map(|p| p.scope().map(|v| (v, p.source())))
            .ok_or_else(|| GriddleConfigError::MissingValue("scope".to_string()))?;

        let verbosity = self
            .partial_configs
            .iter()
            .find_map(|p| p.verbosity().map(|v| (v, p.source())))
            .ok_or_else(|| GriddleConfigError::MissingValue("verbosity".to_string()))?;

        Ok(GriddleConfig {
            signing_key,
            rest_api_endpoint,
            #[cfg(feature = "proxy")]
            proxy_forward_url,
            scope,
            verbosity,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "config-env")]
    use std::collections::HashMap;

    #[cfg(feature = "config-clap")]
    use ::clap::{clap_app, crate_version, ArgMatches};

    #[cfg(feature = "config-clap")]
    use crate::config::ClapPartialGriddleConfigBuilder;
    #[cfg(feature = "config-default")]
    use crate::config::DefaultPartialGriddleConfigBuilder;
    #[cfg(feature = "config-env")]
    use crate::config::{env::tests::GriddleHashmapEnvStore, EnvPartialGriddleConfigBuilder};

    #[cfg(all(
        feature = "config-env",
        feature = "config-default",
        feature = "config-clap"
    ))]
    use crate::config::GriddleConfigSource;
    use crate::config::Scope;

    // Test environment variables
    #[cfg(feature = "config-env")]
    static TEST_SIGNING_KEY_ENV: &str = "GRIDDLE_KEY";
    #[cfg(feature = "config-env")]
    static TEST_REST_API_ENDPOINT_ENV: &str = "GRIDDLE_BIND";
    #[cfg(all(feature = "proxy", feature = "config-env"))]
    const TEST_PROXY_FORWARD_URL_ENV: &str = "GRIDDLE_FORWARD_URL";

    // Example configuration values
    static EXAMPLE_SIGNING_KEY: &str = "test-key";
    static EXAMPLE_REST_API_ENDPOINT: &str = "127.0.0.1:8000";
    static EXAMPLE_SCOPE: &str = "service";
    static EXAMPLE_SCOPE_VARIANT: &Scope = &Scope::Service;
    #[cfg(feature = "proxy")]
    static EXAMPLE_PROXY_FORWARD_URL: &str = "http://127.0.0.1:8080";

    #[cfg(feature = "config-default")]
    // Default configuration values
    static DEFAULT_REST_API_ENDPOINT: &str = "localhost:8000";
    #[cfg(all(feature = "proxy", feature = "config-default"))]
    static DEFAULT_PROXY_FORWARD_URL: &str = "http://localhost:8080";

    #[cfg(feature = "config-clap")]
    // Create an `ArgMatches` object to construct a `ClapPartialGriddleConfigBuilder`
    fn create_arg_matches(args: Vec<&str>) -> ArgMatches<'static> {
        #[cfg(not(feature = "proxy"))]
        {
            clap_app!(griddleconfigtest =>
                (version: crate_version!())
                (about: "Griddle-Config-Test")
                (@arg key: -k --key +takes_value)
                (@arg bind: -b --bind +takes_value)
                (@arg scope: -s --scope +takes_value)
                (@arg verbose: -v +multiple))
            .get_matches_from(args)
        }
        #[cfg(feature = "proxy")]
        {
            clap_app!(griddleconfigtest =>
                (version: crate_version!())
                (about: "Griddle-Config-Test")
                (@arg key: -k --key +takes_value)
                (@arg bind: -b --bind +takes_value)
                (@arg scope: -s --scope +takes_value)
                (@arg verbose: -v +multiple)
                (@arg forward_url: --("forward-url") +takes_value))
            .get_matches_from(args)
        }
    }

    #[cfg(feature = "config-env")]
    #[test]
    /// This test verifies that a `GriddleConfig` object may not be constructed from just
    /// a `EnvPartialGriddleConfigBuilder` object, in the following steps:
    ///
    /// 1. Create a `GriddleHashmapEnvStore` with all configuration values set
    /// 2. Create a `EnvPartialGriddleConfigBuilder` from the mock environment
    /// 3. Create a `GriddleConfigBuilder`, adding the `EnvPartialGriddleConfigBuilder` from step 2
    /// 4. Attempt to create a `GriddleConfig` object from the builder, assert the resulting error
    ///
    fn test_build_config_from_env() {
        // Create a new hashmap-based store to mock environment variables
        let hashmap_env_store = {
            let mut internal = HashMap::new();
            internal.insert(
                TEST_SIGNING_KEY_ENV.to_string(),
                EXAMPLE_SIGNING_KEY.to_string(),
            );
            internal.insert(
                TEST_REST_API_ENDPOINT_ENV.to_string(),
                EXAMPLE_REST_API_ENDPOINT.to_string(),
            );
            #[cfg(feature = "proxy")]
            {
                internal.insert(
                    TEST_PROXY_FORWARD_URL_ENV.to_string(),
                    EXAMPLE_PROXY_FORWARD_URL.to_string(),
                );
            }
            GriddleHashmapEnvStore::new(internal)
        };
        // Create an `EnvPartialGriddleConfigBuilder` from the hashmap, or mock environment
        let env_config_builder = EnvPartialGriddleConfigBuilder::from_store(hashmap_env_store);
        // Create a `GriddleConfigBuilder` and add the partial config from the step above
        let griddle_config_builder = GriddleConfigBuilder::new().with_partial_config(
            env_config_builder
                .build()
                .expect("Unable to build partial config from env"),
        );
        // Build the final `GriddleConfig` object
        let final_config = griddle_config_builder.build();
        // Assert the `GriddleConfig` object was not successfully created
        assert!(final_config.is_err());
    }

    #[cfg(feature = "config-default")]
    #[test]
    /// This test verifies that a `GriddleConfig` object may not be constructed from a
    /// `DefaultPartialGriddleConfigBuilder` object, in the following steps:
    ///
    /// 1. Create a `DefaultPartialGriddleConfigBuilder`, creating a `PartialGriddleConfig`
    /// 2. Create a `GriddleConfigBuilder`, adding the `PartialGriddleConfig` from the step above
    /// 3. Attempt to create a `GriddleConfig` object from the builder, assert the resulting error
    ///
    fn test_build_config_from_default() {
        let default_partial_config = DefaultPartialGriddleConfigBuilder::new()
            .build()
            .expect("Unable to build default `PartialGriddleConfig`");
        // Create a `GriddleConfigBuilder` and add the partial config from above
        let griddle_config_builder =
            GriddleConfigBuilder::new().with_partial_config(default_partial_config);
        // Build the final `GriddleConfig` object
        let final_config = griddle_config_builder.build();
        assert!(final_config.is_err());
    }

    #[cfg(feature = "config-clap")]
    #[test]
    /// This test verifies that a `GriddleConfig` object may be constructed from a
    /// `ClapPartialGriddleConfigBuilder` object, in the following steps:
    ///
    /// 1. Create an `ArgMatches` object to set all configuration values
    /// 2. Create a `ClapPartialGriddleConfigBuilder` from the arguments
    /// 3. Create a `GriddleConfigBuilder`, adding the `ClapPartialGriddleConfigBuilder`
    /// 4. Attempt to create a `GriddleConfig` object from the builder, assert the resulting values
    ///
    fn test_build_config_from_clap() {
        let args = vec![
            "Griddle-Config-Test",
            "-k",
            EXAMPLE_SIGNING_KEY,
            "-b",
            EXAMPLE_REST_API_ENDPOINT,
            "-s",
            EXAMPLE_SCOPE,
            #[cfg(feature = "proxy")]
            "--forward-url",
            #[cfg(feature = "proxy")]
            EXAMPLE_PROXY_FORWARD_URL,
            "-vv",
        ];
        // Create an example `ArgMatches` object to create an `ClapPartialGriddleConfigBuilder`
        let matches = create_arg_matches(args);
        let clap_config_builder = ClapPartialGriddleConfigBuilder::new(matches);
        // Create a `GriddleConfigBuilder` and add the partial builder from above
        let config_builder = GriddleConfigBuilder::new().with_partial_config(
            clap_config_builder
                .build()
                .expect("Unable to build `ClapPartialGriddleConfigBuilder`"),
        );
        let final_config = config_builder.build();
        assert!(final_config.is_ok());
        let config = final_config.unwrap();
        assert_eq!(config.signing_key(), EXAMPLE_SIGNING_KEY);
        assert_eq!(config.rest_api_endpoint(), EXAMPLE_REST_API_ENDPOINT);
        assert_eq!(config.scope(), EXAMPLE_SCOPE_VARIANT);
        #[cfg(feature = "proxy")]
        {
            assert_eq!(config.proxy_forward_url(), EXAMPLE_PROXY_FORWARD_URL);
        }
        assert_eq!(config.verbosity(), log::Level::Debug);
    }

    #[cfg(all(
        feature = "config-env",
        feature = "config-default",
        feature = "config-clap"
    ))]
    #[test]
    /// Verify that a `GriddleConfig` object can be created from various configuration sources
    ///
    /// 1. Create an `ArgMatches` object for some of the config values
    /// 2. Build a `PartialGriddleConfig` from a `ClapPartialGriddleConfigBuilder`, generated
    ///    using the arguments in the first step
    /// 3. Build a `EnvPartialGriddleConfigBuilder` from a mock environment store
    /// 4. Build a `PartialGriddleConfig` from a `DefaultPartialGriddleConfigBuilder`
    /// 5. Create a `GriddleConfigBuilder`, adding first the command line partial config, then the
    ///    environment config and finally the default config object.
    /// 6. Assert the expected values come from the expected source
    fn test_build_config() {
        // Create an `ArgMatches` struct to build the command line partial config
        let matches =
            create_arg_matches(vec!["Griddle-Config-Test", "--scope", EXAMPLE_SCOPE, "-vv"]);
        // Create a `PartialGriddleConfig` object from the `ClapPartialGriddleConfigBuilder`
        let clap_partial_config = ClapPartialGriddleConfigBuilder::new(matches)
            .build()
            .expect("Unable to build `PartialGriddleConfig` from command line args");

        // Create a mock environment to store the `signing_key` configuration value
        let hashmap_env_store = {
            let mut internal = HashMap::new();
            internal.insert(
                TEST_SIGNING_KEY_ENV.to_string(),
                EXAMPLE_SIGNING_KEY.to_string(),
            );
            GriddleHashmapEnvStore::new(internal)
        };
        // Create a `ClapPartialGriddleConfigBuilder` from the mock environment, building it
        // to be a `PartialGriddleConfig` object
        let env_partial_config = EnvPartialGriddleConfigBuilder::from_store(hashmap_env_store)
            .build()
            .expect("Unable to build `PartialGriddleConfig` from mock environment");

        // Create a `PartialGriddleConfig` from the default values
        let default_partial_config = DefaultPartialGriddleConfigBuilder::new()
            .build()
            .expect("Unable to build default `PartialGriddleConfig`");

        // Create a `GriddleConfigBuilder`, first the command line config is added, then the
        // environment-based config, and finally the default configuration object.
        let griddle_config_builder = GriddleConfigBuilder::new()
            .with_partial_config(clap_partial_config)
            .with_partial_config(env_partial_config)
            .with_partial_config(default_partial_config)
            .build();
        // Assert the `GriddleConfigBuilder` can be successfully built from the various configs
        assert!(griddle_config_builder.is_ok());
        let config = griddle_config_builder.unwrap();
        // Assert the values and their respective sources match what is expected
        // Assert the signing key was set from the environment partial config
        assert_eq!(config.signing_key(), EXAMPLE_SIGNING_KEY);
        assert_eq!(
            config.signing_key_source(),
            &GriddleConfigSource::Environment
        );
        // Assert the rest api endpoint was set by the default partial config
        assert_eq!(config.rest_api_endpoint(), DEFAULT_REST_API_ENDPOINT);
        assert_eq!(
            config.rest_api_endpoint_source(),
            &GriddleConfigSource::Default
        );
        // Assert the scope was set by command line partial config
        assert_eq!(config.scope(), EXAMPLE_SCOPE_VARIANT);
        assert_eq!(config.scope_source(), &GriddleConfigSource::CommandLine);
        #[cfg(feature = "proxy")]
        {
            // Assert the proxy forward url was set by the default partial config
            assert_eq!(config.proxy_forward_url(), DEFAULT_PROXY_FORWARD_URL);
            assert_eq!(
                config.proxy_forward_url_source(),
                &GriddleConfigSource::Default
            );
        }
        // Assert the verbosity was set by the command line partial config
        assert_eq!(config.verbosity(), log::Level::Debug);
        assert_eq!(config.verbosity_source(), &GriddleConfigSource::CommandLine);
    }
}

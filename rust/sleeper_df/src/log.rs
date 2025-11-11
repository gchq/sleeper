//! Logging configuration.
/*
 * Copyright 2022-2025 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use std::sync::Once;
use tracing_subscriber::{EnvFilter, filter::LevelFilter};

/// An object guaranteed to only initialise once. Thread safe.
static LOG_CFG: Once = Once::new();

/// A one time initialisation of the logging library.
///
/// This function uses a [`Once`] object to ensure
/// initialisation only happens once. This is safe even
/// if called from multiple threads.
pub fn maybe_cfg_log() {
    LOG_CFG.call_once(|| {
        // Install and configure environment logger
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env().add_directive(LevelFilter::INFO.into()))
            .with_ansi(false)
            .with_line_number(true)
            .init();
    });
}

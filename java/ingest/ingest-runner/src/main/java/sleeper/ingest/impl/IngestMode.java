/*
 * Copyright 2022-2024 Crown Copyright
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

package sleeper.ingest.impl;

/**
 * Determines how {@link IngestCoordinator} creates files while performing an ingest
 * {@link IngestMode#ONE_FILE_PER_LEAF} - Write a new file in each relevant leaf partition
 * {@link IngestMode#ONE_REFERENCE_PER_LEAF} - Writes one file at the root partition, and adds references to that
 * file to all relevant leaf partitions.
 */
public enum IngestMode {
    ONE_FILE_PER_LEAF,
    ONE_REFERENCE_PER_LEAF
}

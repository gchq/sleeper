/*
 * Copyright 2022 Crown Copyright
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
package sleeper.compaction.jobexecution;

/**
 * Result of a scaling out check.
 */
public enum ScaleOutResult {
    /** No scale up required. Cluster has spare container capacity. */
    NOT_REQUIRED,
    /** Scale up required, but already at maximum instances. */
    NO_SPARE_HEADROOM,
    /** Scaling operation already in progress. Can't scale. */
    SCALING_IN_PROGRESS,
    /** Scaling operation has begun. */
    SCALING_INITIATED,
}

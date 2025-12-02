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
package sleeper.cdk.stack.core;

import software.amazon.awscdk.services.iam.IRole;

/**
 * References for roles that can be assumed to interact with Sleeper.
 *
 * @param instanceAdmin a role for admin access to the Sleeper instance
 * @param ingestByQueue a role to add data to Sleeper via SQS queues, by standard ingest or bulk import
 * @param directIngest  a role to add data to Sleeper directly against the underlying AWS resources
 */
public record SleeperInstanceRoles(IRole instanceAdmin, IRole ingestByQueue, IRole directIngest) {
}

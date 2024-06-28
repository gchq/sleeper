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
package sleeper.cdk.stack;

import software.amazon.awscdk.services.iam.IGrantable;
import software.constructs.Construct;

import sleeper.configuration.properties.instance.InstanceProperties;

import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_STATUS_STORE_ENABLED;

public interface CompactionStatusStoreResources {

    default void grantWriteJobEvent(IGrantable grantee) {
    }

    default void grantWriteTaskEvent(IGrantable grantee) {
    }

    static CompactionStatusStoreResources from(Construct scope, InstanceProperties properties, ManagedPoliciesStack policiesStack) {
        if (properties.getBoolean(COMPACTION_STATUS_STORE_ENABLED)) {
            return new DynamoDBCompactionStatusStoreResources(scope, properties, policiesStack);
        } else {
            return none();
        }
    }

    static CompactionStatusStoreResources none() {
        return new CompactionStatusStoreResources() {
        };
    }
}

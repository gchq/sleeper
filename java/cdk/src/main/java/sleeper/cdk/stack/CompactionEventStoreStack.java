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
package sleeper.cdk.stack;

import sleeper.configuration.properties.InstanceProperties;
import software.amazon.awscdk.services.iam.IGrantable;
import software.constructs.Construct;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_EVENT_STORE_ENABLED;

public interface CompactionEventStoreStack {

    default void grantWriteJobEvent(IGrantable grantee) {
    }

    static CompactionEventStoreStack from(Construct scope, InstanceProperties properties) {
        if (properties.getBoolean(COMPACTION_EVENT_STORE_ENABLED)) {
            return new DynamoDBCompactionEventStoreStack(scope, properties);
        } else {
            return none();
        }
    }

    static CompactionEventStoreStack none() {
        return new CompactionEventStoreStack() {
        };
    }
}

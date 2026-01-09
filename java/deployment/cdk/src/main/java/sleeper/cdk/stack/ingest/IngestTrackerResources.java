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

package sleeper.cdk.stack.ingest;

import software.amazon.awscdk.services.iam.IGrantable;
import software.constructs.Construct;

import sleeper.cdk.stack.core.ManagedPoliciesStack;
import sleeper.core.properties.instance.InstanceProperties;

import static sleeper.core.properties.instance.IngestProperty.INGEST_TRACKER_ENABLED;

public interface IngestTrackerResources {

    default void grantWriteJobEvent(IGrantable grantee) {
    }

    default void grantWriteTaskEvent(IGrantable grantee) {
    }

    static IngestTrackerResources from(Construct scope, String id, InstanceProperties properties, ManagedPoliciesStack policiesStack) {
        if (properties.getBoolean(INGEST_TRACKER_ENABLED)) {
            return new IngestTrackerStack(scope, id, properties, policiesStack);
        } else {
            return none();
        }
    }

    static IngestTrackerResources none() {
        return new IngestTrackerResources() {
        };
    }
}

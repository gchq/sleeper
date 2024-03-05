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
package sleeper.cdk;

import software.amazon.awscdk.services.lambda.Tracing;

import sleeper.configuration.properties.instance.InstanceProperties;

import static sleeper.configuration.properties.instance.CommonProperty.XRAY_TRACING_ENABLED;

public class TracingUtils {

    private TracingUtils() {
    }

    public static Tracing active(InstanceProperties properties) {
        if (properties.getBoolean(XRAY_TRACING_ENABLED)) {
            return Tracing.ACTIVE;
        } else {
            return Tracing.DISABLED;
        }
    }

    public static Tracing passThrough(InstanceProperties properties) {
        if (properties.getBoolean(XRAY_TRACING_ENABLED)) {
            return Tracing.PASS_THROUGH;
        } else {
            return Tracing.DISABLED;
        }
    }
}

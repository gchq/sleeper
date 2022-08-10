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
package sleeper.environment.cdk.util;

import software.amazon.awscdk.App;

@FunctionalInterface
public interface AppContext {

    Object get(String key);

    default String getInstanceId() {
        return getStringOrDefault("instanceId", "SleeperEnvironment");
    }

    default String getStringOrDefault(String key, String defaultValue) {
        Object object = get(key);
        if (object instanceof String) {
            return (String) object;
        } else {
            return defaultValue;
        }
    }

    static AppContext of(App app) {
        return app.getNode()::tryGetContext;
    }
}

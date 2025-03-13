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
package sleeper.environment.cdk.config;

import java.util.Optional;

public class RequiredStringParameter {

    private final String key;

    private RequiredStringParameter(String key) {
        this.key = key;
    }

    String get(AppContext context) {
        return OptionalStringParameter.getOptionalString(context, key)
                .orElseThrow(() -> new IllegalArgumentException(key + " is required"));
    }

    public StringValue value(String value) {
        return new StringValue(key, value);
    }

    static RequiredStringParameter key(String key) {
        return new RequiredStringParameter(key);
    }

    static Optional<String> getOptionalString(AppContext context, String key) {
        return Optional.ofNullable(StringParameter.getStringOrDefault(context, key, null));
    }
}

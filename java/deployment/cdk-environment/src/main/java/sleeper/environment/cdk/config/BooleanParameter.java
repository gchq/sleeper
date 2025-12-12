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

public class BooleanParameter {

    private final String key;
    private final boolean defaultValue;

    private BooleanParameter(String key, boolean defaultValue) {
        this.key = key;
        this.defaultValue = defaultValue;
    }

    boolean get(AppContext context) {
        return OptionalStringParameter.getOptionalString(context, key)
                .map(Boolean::parseBoolean)
                .orElse(defaultValue);
    }

    public StringValue value(boolean value) {
        return value("" + value);
    }

    public StringValue value(String value) {
        return new StringValue(key, value);
    }

    static BooleanParameter keyAndDefault(String key, boolean defaultValue) {
        return new BooleanParameter(key, defaultValue);
    }

}

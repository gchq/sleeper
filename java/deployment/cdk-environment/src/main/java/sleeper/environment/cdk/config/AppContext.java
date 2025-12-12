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

import software.constructs.Construct;
import software.constructs.Node;

import java.util.List;
import java.util.Optional;

@FunctionalInterface
public interface AppContext {

    Object get(String key);

    default String get(StringParameter string) {
        return string.get(this);
    }

    default String get(RequiredStringParameter string) {
        return string.get(this);
    }

    default Optional<String> get(OptionalStringParameter string) {
        return string.get(this);
    }

    default List<String> get(StringListParameter list) {
        return list.get(this);
    }

    default boolean get(BooleanParameter bool) {
        return bool.get(this);
    }

    default int get(IntParameter integer) {
        return integer.get(this);
    }

    static AppContext of(Construct construct) {
        return of(construct.getNode());
    }

    static AppContext of(Node node) {
        return node::tryGetContext;
    }

    // Use this for tests, since App and Stack are slow to instantiate
    static AppContext of(StringValue... values) {
        return StringValue.mapOf(values)::get;
    }

    static AppContext empty() {
        return key -> null;
    }
}

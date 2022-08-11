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
package sleeper.environment.cdk.config;

import software.amazon.awscdk.App;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.services.ec2.IVpc;
import software.constructs.Construct;
import software.constructs.Node;

import java.util.Optional;

@FunctionalInterface
public interface AppContext {

    Object get(String key);

    default String get(StringParameter string) {
        return string.get(this);
    }

    default IVpc getOrDefault(VpcParameter vpc, Construct scope, IVpc defaultVpc) {
        return vpc.getOrDefault(this, scope, defaultVpc);
    }

    default String getStringOrDefault(String key, String defaultValue) {
        Object object = get(key);
        if (object instanceof String) {
            return (String) object;
        } else {
            return defaultValue;
        }
    }

    default Optional<String> getStringOpt(String key) {
        return Optional.ofNullable(getStringOrDefault(key, null));
    }

    static AppContext of(App app) {
        return of(app.getNode());
    }

    static AppContext of(Stack stack) {
        return of(stack.getNode());
    }

    static AppContext of(Node node) {
        return node::tryGetContext;
    }
}

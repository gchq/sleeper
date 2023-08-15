/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.systemtest.drivers.util;

import java.lang.reflect.Method;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class TestContext {

    private final String displayName;
    private final Set<String> tags;
    private final Class<?> testClass;
    private final Method testMethod;

    private TestContext(Builder builder) {
        displayName = Objects.requireNonNull(builder.displayName, "displayName must not be null");
        tags = Objects.requireNonNull(builder.tags, "tags must not be null");
        testClass = builder.testClass;
        testMethod = builder.testMethod;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getDisplayName() {
        return displayName;
    }

    public Set<String> getTags() {
        return tags;
    }

    public Optional<Class<?>> getTestClass() {
        return Optional.ofNullable(testClass);
    }

    public Optional<Method> getTestMethod() {
        return Optional.ofNullable(testMethod);
    }

    public static final class Builder {
        private String displayName;
        private Set<String> tags;
        private Class<?> testClass;
        private Method testMethod;

        private Builder() {
        }

        public Builder displayName(String displayName) {
            this.displayName = displayName;
            return this;
        }

        public Builder tags(Set<String> tags) {
            this.tags = tags;
            return this;
        }

        public Builder testClass(Class<?> testClass) {
            this.testClass = testClass;
            return this;
        }

        public Builder testMethod(Method testMethod) {
            this.testMethod = testMethod;
            return this;
        }

        public TestContext build() {
            return new TestContext(this);
        }
    }
}

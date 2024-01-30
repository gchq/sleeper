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

package sleeper.statestore.s3;

import sleeper.core.statestore.StateStoreException;

import java.util.ArrayList;
import java.util.List;

public class ConditionResult {
    private final List<StateStoreException> exceptions;

    private ConditionResult(Builder builder) {
        exceptions = builder.exceptions;
    }

    public static Builder builder() {
        return new Builder();
    }

    public void throwAll() throws StateStoreException {
        if (!exceptions.isEmpty()) {
            throw new ConditionFailedException(exceptions);
        }
    }

    public static final class Builder {
        private final List<StateStoreException> exceptions = new ArrayList<>();

        private Builder() {
        }

        public Builder addException(StateStoreException exceptions) {
            this.exceptions.add(exceptions);
            return this;
        }

        public ConditionResult build() {
            return new ConditionResult(this);
        }
    }
}

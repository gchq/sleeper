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

package sleeper.build.github.containers;

import java.time.Instant;
import java.util.List;

public class GHCRContainer {
    private String id;
    private List<String> tags;
    private Instant created;

    private GHCRContainer(Builder builder) {
        id = builder.id;
        tags = builder.tags;
        created = builder.created;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String id;
        private List<String> tags;
        private Instant created;

        private Builder() {
        }

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder tags(List<String> tags) {
            this.tags = tags;
            return this;
        }

        public Builder created(Instant created) {
            this.created = created;
            return this;
        }

        public GHCRContainer build() {
            return new GHCRContainer(this);
        }
    }
}

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

package sleeper.core.statestore;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class AddFilesRequest {
    private final List<AddFileRequest> files;

    private AddFilesRequest(Builder builder) {
        files = builder.files;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static AddFilesRequest addFiles(Consumer<Builder> config) {
        Builder builder = builder();
        config.accept(builder);
        return builder.build();
    }

    public Stream<FileInfoV2> buildFileInfos() {
        return files.stream()
                .map(AddFileRequest::buildFileInfo);
    }

    public static final class Builder {
        private List<AddFileRequest> files;

        private Builder() {
        }

        public Builder files(List<AddFileRequest> files) {
            this.files = files;
            return this;
        }

        public AddFilesRequest build() {
            return new AddFilesRequest(this);
        }
    }
}

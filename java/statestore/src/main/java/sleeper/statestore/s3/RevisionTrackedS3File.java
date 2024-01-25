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

import java.io.IOException;
import java.util.function.Function;

class RevisionTrackedS3File<T> {

    private final String description;
    private final String revisionIdKey;
    private final Function<S3RevisionId, String> buildPathFromRevisionId;
    private final LoadData<T> loadData;
    private final WriteData<T> writeData;

    private RevisionTrackedS3File(Builder<T> builder) {
        description = builder.description;
        revisionIdKey = builder.revisionIdKey;
        buildPathFromRevisionId = builder.buildPathFromRevisionId;
        loadData = builder.loadData;
        writeData = builder.writeData;
    }

    public static Builder<?> builder() {
        return new Builder<>();
    }

    interface LoadData<T> {
        T load(String path) throws IOException, StateStoreException;
    }

    interface WriteData<T> {
        void write(T data, String path) throws IOException;
    }

    public String getDescription() {
        return description;
    }

    public S3RevisionId getCurrentRevisionId(S3RevisionUtils revisionUtils) {
        return revisionUtils.getCurrentRevisionId(revisionIdKey);
    }

    public void conditionalUpdateOfRevisionId(
            S3RevisionUtils revisionUtils, S3RevisionId revisionId, S3RevisionId nextRevisionId) {
        revisionUtils.conditionalUpdateOfRevisionId(revisionIdKey, revisionId, nextRevisionId);
    }

    public String getPath(S3RevisionId revisionId) {
        return buildPathFromRevisionId.apply(revisionId);
    }

    public T loadData(String path) throws StateStoreException, IOException {
        return loadData.load(path);
    }

    public void writeData(T data, String path) throws IOException {
        writeData.write(data, path);
    }

    static final class Builder<T> {
        private String description;
        private String revisionIdKey;
        private Function<S3RevisionId, String> buildPathFromRevisionId;
        private LoadData<T> loadData;
        private WriteData<T> writeData;

        private Builder() {
        }

        Builder<T> description(String description) {
            this.description = description;
            return this;
        }

        Builder<T> revisionIdKey(String revisionIdKey) {
            this.revisionIdKey = revisionIdKey;
            return this;
        }

        Builder<T> buildPathFromRevisionId(Function<S3RevisionId, String> buildPathFromRevisionId) {
            this.buildPathFromRevisionId = buildPathFromRevisionId;
            return this;
        }

        <N> Builder<N> dataStore(LoadData<N> loadData, WriteData<N> writeData) {
            this.loadData = (LoadData<T>) loadData;
            this.writeData = (WriteData<T>) writeData;
            return (Builder<N>) this;
        }

        RevisionTrackedS3File<T> build() {
            return new RevisionTrackedS3File<>(this);
        }
    }
}

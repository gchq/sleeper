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

import java.util.function.Function;

class RevisionTrackedS3FileType<T> {

    private final String description;
    private final String revisionIdKey;
    private final Function<S3RevisionId, String> buildPathFromRevisionId;
    private final Store<T> store;

    private RevisionTrackedS3FileType(Builder<T> builder) {
        description = builder.description;
        revisionIdKey = builder.revisionIdKey;
        buildPathFromRevisionId = builder.buildPathFromRevisionId;
        store = builder.store;
    }

    public static Builder<?> builder() {
        return new Builder<>();
    }

    interface Store<T> {
        T load(String path) throws StateStoreException;

        void write(T data, String path) throws StateStoreException;

        void delete(String path) throws StateStoreException;
    }

    public String getDescription() {
        return description;
    }

    public String getRevisionIdKey() {
        return revisionIdKey;
    }

    public String getPath(S3RevisionId revisionId) {
        return buildPathFromRevisionId.apply(revisionId);
    }

    public T loadData(String path) throws StateStoreException {
        return store.load(path);
    }

    public void writeData(T data, String path) throws StateStoreException {
        store.write(data, path);
    }

    public void deleteFile(String path) throws StateStoreException {
        store.delete(path);
    }

    static final class Builder<T> {
        private String description;
        private String revisionIdKey;
        private Function<S3RevisionId, String> buildPathFromRevisionId;
        private Store<T> store;

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

        <N> Builder<N> store(Store<N> store) {
            this.store = (Store<T>) store;
            return (Builder<N>) this;
        }

        RevisionTrackedS3FileType<T> build() {
            return new RevisionTrackedS3FileType<>(this);
        }
    }
}

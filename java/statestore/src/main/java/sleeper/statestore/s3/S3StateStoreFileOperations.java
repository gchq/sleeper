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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import sleeper.core.statestore.StateStoreException;

import java.io.IOException;
import java.util.function.Function;

class S3StateStoreFileOperations<T> {

    private final String description;
    private final String revisionIdKey;
    private final Function<S3RevisionId, String> buildPathFromRevisionId;
    private final LoadData<T> loadData;
    private final WriteData<T> writeData;
    private final DeleteFile deleteFile;

    private S3StateStoreFileOperations(Builder<T> builder) {
        description = builder.description;
        revisionIdKey = builder.revisionIdKey;
        buildPathFromRevisionId = builder.buildPathFromRevisionId;
        loadData = builder.loadData;
        writeData = builder.writeData;
        deleteFile = builder.deleteFile;
    }

    public static Builder<?> builder() {
        return new Builder<>();
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
        return loadData.load(path);
    }

    public void writeData(T data, String path) throws StateStoreException {
        writeData.write(data, path);
    }

    public void deleteFile(String path) throws StateStoreException {
        deleteFile.delete(path);
    }

    interface LoadData<T> {
        T load(String path) throws StateStoreException;
    }

    interface WriteData<T> {
        void write(T data, String path) throws StateStoreException;
    }

    interface DeleteFile {
        void delete(String path) throws StateStoreException;
    }

    static final class Builder<T> {
        private String description;
        private String revisionIdKey;
        private Function<S3RevisionId, String> buildPathFromRevisionId;
        private LoadData<T> loadData;
        private WriteData<T> writeData;
        private DeleteFile deleteFile;

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

        <N> Builder<N> loadAndWriteData(LoadData<N> loadData, WriteData<N> writeData) {
            this.loadData = (LoadData<T>) loadData;
            this.writeData = (WriteData<T>) writeData;
            return (Builder<N>) this;
        }

        Builder<T> deleteFile(DeleteFile deleteFile) {
            this.deleteFile = deleteFile;
            return this;
        }

        Builder<T> hadoopConf(Configuration conf) {
            return deleteFile(fileDeleter(conf));
        }

        S3StateStoreFileOperations<T> build() {
            return new S3StateStoreFileOperations<>(this);
        }
    }

    private static DeleteFile fileDeleter(Configuration conf) {
        return pathStr -> {
            Path path = new Path(pathStr);
            try {
                path.getFileSystem(conf).delete(path, false);
            } catch (IOException e1) {
                throw new StateStoreException("Failed to delete file after failing revision ID update", e1);
            }
        };
    }
}

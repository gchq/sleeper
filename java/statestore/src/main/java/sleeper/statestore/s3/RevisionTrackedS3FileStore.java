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

class RevisionTrackedS3FileStore<T> implements S3FileStoreType.Store<T> {
    private final LoadData<T> loadData;
    private final WriteData<T> writeData;
    private final Configuration conf;

    RevisionTrackedS3FileStore(LoadData<T> loadData, WriteData<T> writeData, Configuration conf) {
        this.loadData = loadData;
        this.writeData = writeData;
        this.conf = conf;
    }

    @Override
    public T load(String path) throws StateStoreException {
        return loadData.load(path);
    }

    @Override
    public void write(T data, String path) throws StateStoreException {
        writeData.write(data, path);
    }

    @Override
    public void delete(String pathStr) throws StateStoreException {
        Path path = new Path(pathStr);
        try {
            path.getFileSystem(conf).delete(path, false);
        } catch (IOException e1) {
            throw new StateStoreException("Failed to delete file after failing revision ID update", e1);
        }
    }

    interface LoadData<T> {
        T load(String path) throws StateStoreException;
    }

    interface WriteData<T> {
        void write(T data, String path) throws StateStoreException;
    }
}

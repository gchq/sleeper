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
package sleeper.statestore.transactionlog;

import org.apache.hadoop.conf.Configuration;

import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.transactionlog.StateStoreFiles;
import sleeper.statestore.StateStoreFileUtils;

public class TransactionLogFilesSnapshotSerDe {
    private final StateStoreFileUtils stateStoreFileUtils;

    TransactionLogFilesSnapshotSerDe(Configuration configuration) {
        this.stateStoreFileUtils = StateStoreFileUtils.forFiles(configuration);
    }

    String save(String basePath, StateStoreFiles state, long lastTransactionNumber) throws StateStoreException {
        String filePath = createFilesPath(basePath, lastTransactionNumber);
        stateStoreFileUtils.saveFiles(filePath, state);
        return filePath;
    }

    StateStoreFiles load(TransactionLogSnapshot snapshot) throws StateStoreException {
        return load(snapshot.getPath());
    }

    StateStoreFiles load(String filePath) throws StateStoreException {
        StateStoreFiles files = new StateStoreFiles();
        stateStoreFileUtils.loadFiles(filePath, files::add);
        return files;
    }

    private String createFilesPath(String basePath, long lastTransactionNumber) throws StateStoreException {
        return basePath + "/snapshots/" + lastTransactionNumber + "-files.parquet";
    }
}

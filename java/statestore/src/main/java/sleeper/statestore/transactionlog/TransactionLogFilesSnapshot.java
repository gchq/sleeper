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

import sleeper.core.record.Record;
import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceSerDe;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.transactionlog.StateStoreFiles;
import sleeper.statestore.StateStoreFileUtils;

import java.time.Instant;
import java.util.List;

public class TransactionLogFilesSnapshot {
    private final FileReferenceSerDe serDe = new FileReferenceSerDe();
    private final StateStoreFileUtils stateStoreFileUtils;

    TransactionLogFilesSnapshot(Configuration configuration) {
        this.stateStoreFileUtils = StateStoreFileUtils.forFiles(configuration);
    }

    void save(String basePath, StateStoreFiles state, long lastTransactionNumber) throws StateStoreException {
        String path = createFilesPath(basePath, lastTransactionNumber);
        stateStoreFileUtils.save(path, state.referencedAndUnreferenced().map(this::getRecordFromFile));
    }

    StateStoreFiles load(String basePath, long lastTransactionNumber) throws StateStoreException {
        StateStoreFiles files = new StateStoreFiles();
        stateStoreFileUtils.load(createFilesPath(basePath, lastTransactionNumber))
                .map(this::getFileFromRecord)
                .forEach(files::add);
        return files;
    }

    private Record getRecordFromFile(AllReferencesToAFile file) {
        Record record = new Record();
        record.put("fileName", file.getFilename());
        record.put("referencesJson", serDe.collectionToJson(file.getInternalReferences()));
        record.put("externalReferences", file.getExternalReferenceCount());
        record.put("lastStateStoreUpdateTime", file.getLastStateStoreUpdateTime().toEpochMilli());
        return record;
    }

    private AllReferencesToAFile getFileFromRecord(Record record) {
        List<FileReference> internalReferences = serDe.listFromJson((String) record.get("referencesJson"));
        return AllReferencesToAFile.builder()
                .filename((String) record.get("fileName"))
                .internalReferences(internalReferences)
                .totalReferenceCount((int) record.get("externalReferences") + internalReferences.size())
                .lastStateStoreUpdateTime(Instant.ofEpochMilli((long) record.get("lastStateStoreUpdateTime")))
                .build();
    }

    private String createFilesPath(String basePath, long lastTransactionNumber) throws StateStoreException {
        return basePath + "/snapshots/" + lastTransactionNumber + "-files.parquet";
    }
}

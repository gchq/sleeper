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

package sleeper.systemtest.suite.dsl.ingest;

import sleeper.core.record.Record;
import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.FileReference;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;

public class SystemTestIngestCreateFile {

    private final SleeperInstanceContext instance;
    private List<Record> records = new ArrayList<>();
    private List<Reference> references = new ArrayList<>();

    public SystemTestIngestCreateFile(SleeperInstanceContext instance) {
        this.instance = instance;
    }

    public SystemTestIngestCreateFile numberedRecords(LongStream numbers) {
        records = instance.generateNumberedRecords(numbers)
                .collect(toUnmodifiableList());
        return this;
    }

    public SystemTestIngestCreateFile splitEvenlyAcrossPartitionsWithRecordEstimates(String... partitionIds) {
        references = Stream.of(partitionIds)
                .map(partitionId -> Reference.approximatePartitionRecords(
                        partitionId, records.size() / partitionIds.length))
                .collect(toUnmodifiableList());
        return this;
    }

    AllReferencesToAFile buildWithFilename(String filename) {
        return AllReferencesToAFile.builder()
                .filename(filename)
                .totalReferenceCount(references.size())
                .internalReferences(references.stream()
                        .map(reference -> reference.buildWithFilename(filename)))
                .build();
    }

    private static class Reference {
        private final String partitionId;
        private final Long numberOfRecords;
        private final boolean countApproximate;

        Reference(String partitionId, Long numberOfRecords, boolean countApproximate) {
            this.partitionId = partitionId;
            this.numberOfRecords = numberOfRecords;
            this.countApproximate = countApproximate;
        }

        static Reference approximatePartitionRecords(String partitionId, long numberOfRecords) {
            return new Reference(partitionId, numberOfRecords, true);
        }

        FileReference buildWithFilename(String filename) {
            return FileReference.builder()
                    .filename(filename)
                    .partitionId(partitionId)
                    .numberOfRecords(numberOfRecords)
                    .countApproximate(countApproximate)
                    .build();
        }
    }
}

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

package sleeper.ingest.testutils;

import sleeper.core.record.Record;
import sleeper.ingest.impl.IngestCoordinator;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordBatchFactory;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordWriterAcceptingRecords;

import java.util.function.Consumer;

import static sleeper.ingest.testutils.IngestCoordinatorFactory.ingestCoordinatorDirectWriteBackedByArrayList;
import static sleeper.ingest.testutils.IngestCoordinatorFactory.ingestCoordinatorDirectWriteBackedByArrow;

public class TestIngestType {

    private final CoordinatorFactory coordinatorFactory;
    private final GetFilePrefix getFilePrefix;

    private TestIngestType(CoordinatorFactory coordinatorFactory, GetFilePrefix getFilePrefix) {
        this.coordinatorFactory = coordinatorFactory;
        this.getFilePrefix = getFilePrefix;
    }

    public IngestCoordinator<Record> createIngestCoordinator(IngestCoordinatorTestParameters parameters) {
        return coordinatorFactory.createIngestCoordinator(parameters);
    }

    public String getFilePrefix(IngestCoordinatorTestParameters parameters) {
        return getFilePrefix.getFilePrefix(parameters);
    }

    public static TestIngestType directWriteBackedByArrowWriteToLocalFile(
            Consumer<ArrowRecordBatchFactory.Builder<Record>> arrowConfig) {
        return new TestIngestType(
                parameters -> ingestCoordinatorDirectWriteBackedByArrow(parameters, parameters.getLocalFilePrefix(),
                        arrowConfig, new ArrowRecordWriterAcceptingRecords()),
                IngestCoordinatorTestParameters::getLocalFilePrefix);
    }

    public static TestIngestType directWriteBackedByArrowWriteToLocalFile() {
        return directWriteBackedByArrowWriteToLocalFile(arrow -> {
        });
    }

    public static TestIngestType directWriteBackedByArrowWriteToS3() {
        return new TestIngestType(
                parameters -> ingestCoordinatorDirectWriteBackedByArrow(parameters, parameters.getAsyncS3Prefix()),
                IngestCoordinatorTestParameters::getAsyncS3Prefix);
    }

    public static TestIngestType asyncWriteBackedByArrow() {
        return new TestIngestType(
                IngestCoordinatorFactory::ingestCoordinatorAsyncWriteBackedByArrow,
                IngestCoordinatorTestParameters::getAsyncS3Prefix);
    }

    public static TestIngestType directWriteBackedByArrayListWriteToLocalFile(
            int maxRecordsInMemory, long maxRecordsToWriteToLocalStore) {
        return new TestIngestType(
                parameters -> ingestCoordinatorDirectWriteBackedByArrayList(parameters, parameters.getLocalFilePrefix(), maxRecordsInMemory, maxRecordsToWriteToLocalStore),
                IngestCoordinatorTestParameters::getLocalFilePrefix);
    }

    public static TestIngestType directWriteBackedByArrayListWriteToLocalFile() {
        return new TestIngestType(
                parameters -> ingestCoordinatorDirectWriteBackedByArrayList(parameters, parameters.getLocalFilePrefix()),
                IngestCoordinatorTestParameters::getLocalFilePrefix);
    }

    public static TestIngestType directWriteBackedByArrayListWriteToS3() {
        return new TestIngestType(
                parameters -> ingestCoordinatorDirectWriteBackedByArrayList(parameters, parameters.getAsyncS3Prefix()),
                IngestCoordinatorTestParameters::getAsyncS3Prefix);
    }

    private interface CoordinatorFactory {
        IngestCoordinator<Record> createIngestCoordinator(IngestCoordinatorTestParameters parameters);
    }

    private interface GetFilePrefix {
        String getFilePrefix(IngestCoordinatorTestParameters parameters);
    }
}

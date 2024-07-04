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

    public static TestIngestType withConfig(TestIngestConfig config) {
        return new TestIngestType(config::buildCoordinator, config::getFilePrefix);
    }

    public static TestIngestType directWriteBackedByArrowWriteToLocalFile() {
        return withConfig(new TestIngestConfig().localDirectWrite().backedByArrow());
    }

    public static TestIngestType directWriteBackedByArrowWriteToS3() {
        return withConfig(new TestIngestConfig().s3DirectWrite().backedByArrow());
    }

    public static TestIngestType asyncWriteBackedByArrow() {
        return withConfig(new TestIngestConfig().s3AsyncWrite().backedByArrow());
    }

    public static TestIngestType directWriteBackedByArrayListWriteToLocalFile() {
        return withConfig(new TestIngestConfig().localDirectWrite().backedByArrayList());
    }

    public static TestIngestType directWriteBackedByArrayListWriteToS3() {
        return withConfig(new TestIngestConfig().s3DirectWrite().backedByArrayList());
    }

    private interface CoordinatorFactory {
        IngestCoordinator<Record> createIngestCoordinator(IngestCoordinatorTestParameters parameters);
    }

    private interface GetFilePrefix {
        String getFilePrefix(IngestCoordinatorTestParameters parameters);
    }
}

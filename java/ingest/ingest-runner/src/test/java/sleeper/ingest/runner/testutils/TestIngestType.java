/*
 * Copyright 2022-2025 Crown Copyright
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

package sleeper.ingest.runner.testutils;

import sleeper.core.row.Row;
import sleeper.ingest.runner.impl.IngestCoordinator;

public class TestIngestType {

    private final CoordinatorFactory coordinatorFactory;
    private final WriteTarget writeTarget;

    private TestIngestType(CoordinatorFactory coordinatorFactory, WriteTarget writeTarget) {
        this.coordinatorFactory = coordinatorFactory;
        this.writeTarget = writeTarget;
    }

    public IngestCoordinator<Row> createIngestCoordinator(IngestCoordinatorTestParameters parameters) {
        return coordinatorFactory.createIngestCoordinator(parameters);
    }

    public String getFilePrefix(IngestCoordinatorTestParameters parameters) {
        if (writeTarget == WriteTarget.S3) {
            return parameters.getS3Prefix();
        } else {
            return parameters.getLocalFilePrefix();
        }
    }

    public WriteTarget getWriteTarget() {
        return writeTarget;
    }

    public static TestIngestType directWriteBackedByArrowWriteToLocalFile() {
        return new TestIngestType(
                parameters -> parameters.toBuilder().localDirectWrite().backedByArrow().buildCoordinator(),
                WriteTarget.LOCAL);
    }

    public static TestIngestType directWriteBackedByArrowWriteToS3() {
        return new TestIngestType(
                parameters -> parameters.toBuilder().s3DirectWrite().backedByArrow().buildCoordinator(),
                WriteTarget.S3);
    }

    public static TestIngestType asyncWriteBackedByArrow() {
        return new TestIngestType(
                parameters -> parameters.toBuilder().s3AsyncWrite().backedByArrow().buildCoordinator(),
                WriteTarget.S3);
    }

    public static TestIngestType directWriteBackedByArrayListWriteToLocalFile() {
        return new TestIngestType(
                parameters -> parameters.toBuilder().localDirectWrite().backedByArrayList().buildCoordinator(),
                WriteTarget.LOCAL);
    }

    public static TestIngestType directWriteBackedByArrayListWriteToS3() {
        return new TestIngestType(
                parameters -> parameters.toBuilder().s3DirectWrite().backedByArrayList().buildCoordinator(),
                WriteTarget.S3);
    }

    private interface CoordinatorFactory {
        IngestCoordinator<Row> createIngestCoordinator(IngestCoordinatorTestParameters parameters);
    }

    public enum WriteTarget {
        LOCAL,
        S3
    }
}

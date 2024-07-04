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

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.record.Record;
import sleeper.ingest.impl.IngestCoordinator;

import java.util.function.Consumer;

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

    public static Builder builder() {
        return new Builder();
    }

    public static TestIngestType directWriteBackedByArrowWriteToLocalFile() {
        return builder().localDirectWrite().backedByArrow().build();
    }

    public static TestIngestType directWriteBackedByArrowWriteToS3() {
        return builder().s3DirectWrite().backedByArrow().build();
    }

    public static TestIngestType asyncWriteBackedByArrow() {
        return builder().s3AsyncWrite().backedByArrow().build();
    }

    public static TestIngestType directWriteBackedByArrayListWriteToLocalFile() {
        return builder().localDirectWrite().backedByArrayList().build();
    }

    public static TestIngestType directWriteBackedByArrayListWriteToS3() {
        return builder().s3DirectWrite().backedByArrayList().build();
    }

    private interface CoordinatorFactory {
        IngestCoordinator<Record> createIngestCoordinator(IngestCoordinatorTestParameters parameters);
    }

    private interface GetFilePrefix {
        String getFilePrefix(IngestCoordinatorTestParameters parameters);
    }

    public static class Builder {

        private Consumer<IngestCoordinatorTestParameters.Builder> config = builder -> {
        };
        private GetFilePrefix getFilePrefix;

        private Builder() {
        }

        public Builder backedByArrow() {
            config = config.andThen(builder -> builder.backedByArrow());
            return this;
        }

        public Builder backedByArrayList() {
            config = config.andThen(builder -> builder.backedByArrow());
            return this;
        }

        public Builder localDirectWrite() {
            config = config.andThen(builder -> builder.localDirectWrite());
            getFilePrefix = IngestCoordinatorTestParameters::getLocalFilePrefix;
            return this;
        }

        public Builder s3DirectWrite() {
            config = config.andThen(builder -> builder.s3DirectWrite());
            getFilePrefix = IngestCoordinatorTestParameters::getS3Prefix;
            return this;
        }

        public Builder s3AsyncWrite() {
            config = config.andThen(builder -> builder.s3AsyncWrite());
            getFilePrefix = IngestCoordinatorTestParameters::getS3Prefix;
            return this;
        }

        public Builder setInstanceProperties(Consumer<InstanceProperties> setProperties) {
            config = config.andThen(builder -> builder.setInstanceProperties(setProperties));
            return this;
        }

        public IngestCoordinator<Record> buildCoordinator(IngestCoordinatorTestParameters parameters) {
            IngestCoordinatorTestParameters.Builder builder = parameters.toBuilder();
            config.accept(builder);
            return builder.buildCoordinator();
        }

        public String getFilePrefix(IngestCoordinatorTestParameters parameters) {
            return getFilePrefix.getFilePrefix(parameters);
        }

        public TestIngestType build() {
            return new TestIngestType(this::buildCoordinator, getFilePrefix);
        }
    }
}

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
import sleeper.ingest.testutils.IngestCoordinatorTestParameters.CoordinatorConfig;

import java.util.function.Consumer;
import java.util.function.Function;

public class TestIngestConfig {

    private Consumer<CoordinatorConfig> configureCoordinator = config -> {
    };
    private Function<IngestCoordinatorTestParameters, String> getFilePrefix;

    public TestIngestConfig backedByArrow() {
        configureCoordinator = configureCoordinator.andThen(config -> config.backedByArrow());
        return this;
    }

    public TestIngestConfig backedByArrayList() {
        configureCoordinator = configureCoordinator.andThen(config -> config.backedByArrow());
        return this;
    }

    public TestIngestConfig localDirectWrite() {
        configureCoordinator = configureCoordinator.andThen(config -> config.localDirectWrite());
        getFilePrefix = IngestCoordinatorTestParameters::getLocalFilePrefix;
        return this;
    }

    public TestIngestConfig s3DirectWrite() {
        configureCoordinator = configureCoordinator.andThen(config -> config.s3DirectWrite());
        getFilePrefix = IngestCoordinatorTestParameters::getS3Prefix;
        return this;
    }

    public TestIngestConfig s3AsyncWrite() {
        configureCoordinator = configureCoordinator.andThen(config -> config.s3AsyncWrite());
        getFilePrefix = IngestCoordinatorTestParameters::getS3Prefix;
        return this;
    }

    public TestIngestConfig setInstanceProperties(Consumer<InstanceProperties> setProperties) {
        configureCoordinator = configureCoordinator.andThen(config -> config.setInstanceProperties(setProperties));
        return this;
    }

    public IngestCoordinator<Record> buildCoordinator(IngestCoordinatorTestParameters parameters) {
        CoordinatorConfig config = parameters.ingestCoordinatorConfig();
        configureCoordinator.accept(config);
        return config.buildCoordinator();
    }

    public String getFilePrefix(IngestCoordinatorTestParameters parameters) {
        return getFilePrefix.apply(parameters);
    }

}

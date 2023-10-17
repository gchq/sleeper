/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.ingest.job;

import org.apache.hadoop.conf.Configuration;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.ingest.job.status.IngestJobStatusStore;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

import static sleeper.ingest.job.IngestJobValidationUtils.deserialiseAndValidate;
import static sleeper.ingest.job.IngestJobValidationUtils.expandDirectoriesAndUpdateJob;

public class IngestJobMessageHandler {
    private final Configuration configuration;
    private final InstanceProperties instanceProperties;
    private final IngestJobStatusStore ingestJobStatusStore;
    private final Supplier<String> invalidJobIdSupplier;
    private final Supplier<Instant> timeSupplier;

    public IngestJobMessageHandler(Configuration configuration, InstanceProperties instanceProperties,
                                   IngestJobStatusStore ingestJobStatusStore) {
        this(configuration, instanceProperties, ingestJobStatusStore, () -> UUID.randomUUID().toString(), Instant::now);

    }

    public IngestJobMessageHandler(Configuration configuration, InstanceProperties instanceProperties,
                                   IngestJobStatusStore ingestJobStatusStore,
                                   Supplier<String> invalidJobIdSupplier, Supplier<Instant> timeSupplier) {
        this.configuration = configuration;
        this.instanceProperties = instanceProperties;
        this.ingestJobStatusStore = ingestJobStatusStore;
        this.invalidJobIdSupplier = invalidJobIdSupplier;
        this.timeSupplier = timeSupplier;
    }

    public Optional<IngestJob> handleMessage(String message) {
        return deserialiseAndValidate(message, new IngestJobSerDe()::fromJson,
                IngestJob::getValidationFailures, ingestJobStatusStore, invalidJobIdSupplier, timeSupplier)
                .flatMap(job -> expandDirectoriesAndUpdateJob(job.getFiles(), configuration, instanceProperties,
                        files -> job.toBuilder().files(files).build()));
    }
}

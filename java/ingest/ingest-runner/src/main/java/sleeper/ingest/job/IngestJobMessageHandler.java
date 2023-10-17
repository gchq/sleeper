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

public class IngestJobMessageHandler {
    private final IngestJobValidationUtils ingestJobValidationUtils;

    public IngestJobMessageHandler(Configuration configuration, InstanceProperties instanceProperties,
                                   IngestJobStatusStore ingestJobStatusStore) {
        this(configuration, instanceProperties, ingestJobStatusStore, () -> UUID.randomUUID().toString(), Instant::now);

    }

    public IngestJobMessageHandler(Configuration configuration, InstanceProperties instanceProperties,
                                   IngestJobStatusStore ingestJobStatusStore,
                                   Supplier<String> invalidJobIdSupplier, Supplier<Instant> timeSupplier) {
        this.ingestJobValidationUtils = new IngestJobValidationUtils(ingestJobStatusStore, invalidJobIdSupplier,
                timeSupplier, configuration, instanceProperties);
    }

    public Optional<IngestJob> handleMessage(String message) {
        return ingestJobValidationUtils.deserialiseAndValidate(message,
                        new IngestJobSerDe()::fromJson, IngestJob::getValidationFailures)
                .flatMap(job -> ingestJobValidationUtils.expandDirectories(job,
                        files -> job.toBuilder().files(files).build()));
    }
}

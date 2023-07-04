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

import com.google.gson.JsonParseException;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.utils.HadoopPathUtils;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

import static sleeper.ingest.job.status.IngestJobValidatedEvent.ingestJobRejected;

public class IngestJobMessageHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestJobMessageHandler.class);
    private final Configuration configuration;
    private final InstanceProperties instanceProperties;
    private final IngestJobStatusStore ingestJobStatusStore;
    private final Supplier<String> invalidJobIdSupplier;
    private final Supplier<Instant> timeSuppler;

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
        this.timeSuppler = timeSupplier;
    }

    public Optional<IngestJob> handleMessage(String message) {
        try {
            IngestJob ingestJob = new IngestJobSerDe().fromJson(message);
            return expandDirectories(ingestJob);
        } catch (JsonParseException e) {
            LOGGER.error("Could not deserialize message {}, ", message, e);
            ingestJobStatusStore.jobValidated(ingestJobRejected(invalidJobIdSupplier.get(), message, timeSuppler.get(),
                    "Error parsing JSON. Reason: " + e.getCause().getMessage()));
            return Optional.empty();
        }
    }

    private Optional<IngestJob> expandDirectories(IngestJob ingestJob) {
        List<String> files = HadoopPathUtils.expandDirectories(ingestJob.getFiles(), configuration, instanceProperties);
        if (files.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(ingestJob.toBuilder().files(files).build());
    }
}

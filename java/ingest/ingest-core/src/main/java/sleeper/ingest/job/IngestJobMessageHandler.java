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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.job.status.IngestJobValidatedEvent;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static sleeper.ingest.job.status.IngestJobValidatedEvent.ingestJobRejected;

public class IngestJobMessageHandler<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestJobMessageHandler.class);
    private final IngestJobStatusStore ingestJobStatusStore;
    private final Function<String, T> deserialiser;
    private final Function<T, IngestJob> toIngestJob;
    private final BiFunction<T, IngestJob, T> applyIngestJobChanges;
    private final Function<List<String>, List<String>> expandDirectories;
    private final Supplier<String> jobIdSupplier;
    private final Supplier<Instant> timeSupplier;

    private IngestJobMessageHandler(Builder<T> builder) {
        ingestJobStatusStore = builder.ingestJobStatusStore;
        deserialiser = builder.deserialiser;
        toIngestJob = builder.toIngestJob;
        applyIngestJobChanges = builder.applyIngestJobChanges;
        expandDirectories = builder.expandDirectories;
        jobIdSupplier = builder.jobIdSupplier;
        timeSupplier = builder.timeSupplier;
    }

    public static Builder<IngestJob> forIngestJob() {
        return builder()
                .deserialiser(new IngestJobSerDe()::fromJson)
                .toIngestJob(job -> job)
                .applyIngestJobChanges((job, changedJob) -> changedJob);
    }

    public static Builder<?> builder() {
        return new Builder<>();
    }

    public Optional<T> deserialiseAndValidate(String message) {
        T job;
        try {
            job = deserialiser.apply(message);
            LOGGER.info("Deserialised message to ingest job {}", job);
        } catch (RuntimeException e) {
            LOGGER.warn("Deserialisation failed for message: {}", message, e);
            ingestJobStatusStore.jobValidated(
                    ingestJobRejected(jobIdSupplier.get(), message, timeSupplier.get(),
                            "Error parsing JSON. Reason: " + Optional.ofNullable(e.getCause()).orElse(e).getMessage()));
            return Optional.empty();
        }
        IngestJob ingestJob = toIngestJob.apply(job);
        IngestJob.Builder ingestJobBuilder = ingestJob.toBuilder();
        String jobId = ingestJob.getId();
        if (jobId == null || jobId.isBlank()) {
            jobId = jobIdSupplier.get();
            LOGGER.info("Null or blank id provided. Generated new id: {}", jobId);
            ingestJobBuilder.id(jobId);
        }

        List<String> files = ingestJob.getFiles();
        List<String> validationFailures = new ArrayList<>();
        if (files == null) {
            validationFailures.add("Missing property \"files\"");
        } else if (files.contains(null)) {
            validationFailures.add("One of the files was null");
        }
        if (ingestJob.getTableName() == null) {
            validationFailures.add("Missing property \"tableName\"");
        }
        if (!validationFailures.isEmpty()) {
            LOGGER.warn("Validation failed: {}", validationFailures);
            ingestJobStatusStore.jobValidated(
                    refusedEventBuilder()
                            .jobId(jobId)
                            .tableName(ingestJob.getTableName())
                            .jsonMessage(message)
                            .reasons(validationFailures.stream()
                                    .map(failure -> "Model validation failed. " + failure)
                                    .collect(Collectors.toUnmodifiableList()))
                            .build());
            return Optional.empty();
        }

        List<String> expandedFiles = expandDirectories.apply(files);
        if (expandedFiles.isEmpty()) {
            LOGGER.warn("Could not find one or more files for job: {}", job);
            ingestJobStatusStore.jobValidated(
                    ingestJobRejected(jobId, message, timeSupplier.get(), "Could not find one or more files"));
            return Optional.empty();
        }

        LOGGER.info("No validation failures found");
        return Optional.of(applyIngestJobChanges.apply(job, ingestJob.toBuilder().id(jobId).files(expandedFiles).build()));
    }

    private IngestJobValidatedEvent.Builder refusedEventBuilder() {
        return IngestJobValidatedEvent.builder()
                .validationTime(timeSupplier.get());
    }

    public static final class Builder<T> {
        private IngestJobStatusStore ingestJobStatusStore;
        private Function<String, T> deserialiser;
        private Function<T, IngestJob> toIngestJob;
        private BiFunction<T, IngestJob, T> applyIngestJobChanges;
        private Function<List<String>, List<String>> expandDirectories;
        private Supplier<String> jobIdSupplier = () -> UUID.randomUUID().toString();
        private Supplier<Instant> timeSupplier = Instant::now;

        private Builder() {
        }

        public Builder<T> ingestJobStatusStore(IngestJobStatusStore ingestJobStatusStore) {
            this.ingestJobStatusStore = ingestJobStatusStore;
            return this;
        }

        public <N> Builder<N> deserialiser(Function<String, N> deserialiser) {
            this.deserialiser = (Function<String, T>) deserialiser;
            return (Builder<N>) this;
        }

        public Builder<T> toIngestJob(Function<T, IngestJob> toIngestJob) {
            this.toIngestJob = toIngestJob;
            return this;
        }

        public Builder<T> applyIngestJobChanges(BiFunction<T, IngestJob, T> applyIngestJobChanges) {
            this.applyIngestJobChanges = applyIngestJobChanges;
            return this;
        }

        public Builder<T> expandDirectories(Function<List<String>, List<String>> expandDirectories) {
            this.expandDirectories = expandDirectories;
            return this;
        }

        public Builder<T> jobIdSupplier(Supplier<String> jobIdSupplier) {
            this.jobIdSupplier = jobIdSupplier;
            return this;
        }

        public Builder<T> timeSupplier(Supplier<Instant> timeSupplier) {
            this.timeSupplier = timeSupplier;
            return this;
        }

        public IngestJobMessageHandler<T> build() {
            return new IngestJobMessageHandler<>(this);
        }
    }
}

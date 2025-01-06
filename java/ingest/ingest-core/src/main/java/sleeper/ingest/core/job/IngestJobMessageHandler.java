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

package sleeper.ingest.core.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.table.TableIndex;
import sleeper.core.table.TableStatus;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.core.tracker.ingest.job.update.IngestJobValidatedEvent;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static sleeper.core.tracker.ingest.job.update.IngestJobValidatedEvent.ingestJobRejected;

/**
 * Deserialises and validates a JSON string to a type of ingest job. Any validation failures are recorded in the
 * {@link IngestJobTracker}.
 *
 * @param <T> the type of ingest job
 */
public class IngestJobMessageHandler<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestJobMessageHandler.class);
    private final TableIndex tableIndex;
    private final IngestJobTracker ingestJobTracker;
    private final Function<String, T> deserialiser;
    private final Function<T, IngestJob> toIngestJob;
    private final BiFunction<T, IngestJob, T> applyIngestJobChanges;
    private final Function<List<String>, List<String>> expandDirectories;
    private final Supplier<String> jobIdSupplier;
    private final Supplier<Instant> timeSupplier;

    private IngestJobMessageHandler(Builder<T> builder) {
        tableIndex = Objects.requireNonNull(builder.tableIndex, "tableIndex must not be null");
        ingestJobTracker = Objects.requireNonNull(builder.ingestJobTracker, "ingestJobTracker must not be null");
        deserialiser = Objects.requireNonNull(builder.deserialiser, "deserialiser must not be null");
        toIngestJob = Objects.requireNonNull(builder.toIngestJob, "toIngestJob must not be null");
        applyIngestJobChanges = Objects.requireNonNull(builder.applyIngestJobChanges, "applyIngestJobChanges must not be null");
        expandDirectories = Objects.requireNonNull(builder.expandDirectories, "expandDirectories must not be null");
        jobIdSupplier = Objects.requireNonNull(builder.jobIdSupplier, "jobIdSupplier must not be null");
        timeSupplier = Objects.requireNonNull(builder.timeSupplier, "timeSupplier must not be null");
    }

    /**
     * Creates a builder for this class configured to deserialise ingest jobs.
     *
     * @return a builder for this class
     */
    public static Builder<IngestJob> forIngestJob() {
        return builder()
                .deserialiser(new IngestJobSerDe()::fromJson)
                .toIngestJob(job -> job)
                .applyIngestJobChanges((job, changedJob) -> changedJob);
    }

    public static Builder<?> builder() {
        return new Builder<>();
    }

    /**
     * Deserialise a JSON string to an ingest job. This also performs validation on the job, which includes the
     * following:
     * - Checks if the files provided in the job exist. If the input files are directories, they will be expanded and
     * files inside these directories will be added to the job. If no files exist the job will be rejected.
     * - Finds the table ID of the table from the table name provided by the job. If no table exists with that name the
     * job will be rejected.
     * - Generates a random job ID if one was not provided by the job. Jobs that have been deserialised without a job ID
     * will not fail validation.
     *
     * @param  message the JSON string
     * @return         an optional containing the validated job, or an empty optional if the deserialisation or
     *                 validation failed
     */
    public Optional<T> deserialiseAndValidate(String message) {
        T job;
        try {
            job = deserialiser.apply(message);
            LOGGER.info("Deserialised message to ingest job {}", job);
        } catch (RuntimeException e) {
            LOGGER.warn("Deserialisation failed for message: {}", message, e);
            ingestJobTracker.jobValidated(
                    ingestJobRejected(jobIdSupplier.get(), message, timeSupplier.get(),
                            "Error parsing JSON. Reason: " + Optional.ofNullable(e.getCause()).orElse(e).getMessage()));
            return Optional.empty();
        }
        return validate(job, message);
    }

    /**
     * Validates an ingest or bulk import job, resolves the Sleeper table and assigns a job ID if not provided.
     *
     * @param  job     the job
     * @param  message the JSON of the original message
     * @return         the resolved job if it validated
     */
    public Optional<T> validate(T job, String message) {
        IngestJob ingestJob = toIngestJob.apply(job);
        String jobId = ingestJob.getId();
        if (jobId == null || jobId.isBlank()) {
            jobId = jobIdSupplier.get();
            LOGGER.info("Null or blank id provided. Generated new id: {}", jobId);
        }

        List<String> files = ingestJob.getFiles();
        List<String> validationFailures = new ArrayList<>();
        if (files == null) {
            validationFailures.add("Missing property \"files\"");
        } else if (files.stream().anyMatch(file -> file == null)) {
            validationFailures.add("One of the files was null");
        }
        Optional<TableStatus> tableOpt = getTable(ingestJob);
        if (tableOpt.isEmpty()) {
            validationFailures.add("Table not found");
        }
        if (!validationFailures.isEmpty()) {
            LOGGER.warn("Validation failed: {}", validationFailures);
            ingestJobTracker.jobValidated(
                    refusedEventBuilder()
                            .jobId(jobId)
                            .tableId(tableOpt.map(TableStatus::getTableUniqueId).orElse(null))
                            .jsonMessage(message)
                            .reasons(validationFailures)
                            .build());
            return Optional.empty();
        }
        TableStatus table = tableOpt.get();

        List<String> expandedFiles = expandDirectories.apply(files);
        if (expandedFiles.isEmpty()) {
            LOGGER.warn("Could not find one or more files for job: {}", job);
            ingestJobTracker.jobValidated(
                    refusedEventBuilder()
                            .jobId(jobId)
                            .tableId(table.getTableUniqueId())
                            .jsonMessage(message)
                            .reasons("Could not find one or more files")
                            .build());
            return Optional.empty();
        }

        LOGGER.info("No validation failures found");
        return Optional.of(applyIngestJobChanges.apply(job,
                ingestJob.toBuilder()
                        .id(jobId)
                        .tableName(table.getTableName())
                        .tableId(table.getTableUniqueId())
                        .files(expandedFiles)
                        .build()));
    }

    private Optional<TableStatus> getTable(IngestJob job) {
        if (job.getTableId() != null) {
            return tableIndex.getTableByUniqueId(job.getTableId());
        } else if (job.getTableName() != null) {
            return tableIndex.getTableByName(job.getTableName());
        } else {
            return Optional.empty();
        }
    }

    private IngestJobValidatedEvent.Builder refusedEventBuilder() {
        return IngestJobValidatedEvent.builder()
                .validationTime(timeSupplier.get());
    }

    /**
     * Builder for creating ingest job message handler objects.
     *
     * @param <T> the type of job to deserialise and validate
     */
    public static final class Builder<T> {
        private TableIndex tableIndex;
        private IngestJobTracker ingestJobTracker;
        private Function<String, T> deserialiser;
        private Function<T, IngestJob> toIngestJob;
        private BiFunction<T, IngestJob, T> applyIngestJobChanges;
        private Function<List<String>, List<String>> expandDirectories;
        private Supplier<String> jobIdSupplier = () -> UUID.randomUUID().toString();
        private Supplier<Instant> timeSupplier = Instant::now;

        private Builder() {
        }

        /**
         * Sets the table index.
         *
         * @param  tableIndex the table index
         * @return            the builder
         */
        public Builder<T> tableIndex(TableIndex tableIndex) {
            this.tableIndex = tableIndex;
            return this;
        }

        /**
         * Sets the ingest job tracker.
         *
         * @param  ingestJobTracker the ingest job tracker
         * @return                  the builder
         */
        public Builder<T> ingestJobTracker(IngestJobTracker ingestJobTracker) {
            this.ingestJobTracker = ingestJobTracker;
            return this;
        }

        /**
         * Sets the deserialiser.
         *
         * @param  deserialiser the deserialiser
         * @return              the builder
         */
        public <N> Builder<N> deserialiser(Function<String, N> deserialiser) {
            this.deserialiser = (Function<String, T>) deserialiser;
            return (Builder<N>) this;
        }

        /**
         * Sets the function that converts the generic ingest job to an ingest job.
         *
         * @param  toIngestJob the function that converts the generic ingest job to an ingest job
         * @return             the builder
         */
        public Builder<T> toIngestJob(Function<T, IngestJob> toIngestJob) {
            this.toIngestJob = toIngestJob;
            return this;
        }

        /**
         * Sets the function that applies changes to the generic ingest job after deserialisation and validation.
         *
         * @param  applyIngestJobChanges the function that applies changes to the generic ingest job
         * @return                       the builder
         */
        public Builder<T> applyIngestJobChanges(BiFunction<T, IngestJob, T> applyIngestJobChanges) {
            this.applyIngestJobChanges = applyIngestJobChanges;
            return this;
        }

        /**
         * Sets the function that expands directories.
         *
         * @param  expandDirectories the function that expands directories
         * @return                   the builder
         */
        public Builder<T> expandDirectories(Function<List<String>, List<String>> expandDirectories) {
            this.expandDirectories = expandDirectories;
            return this;
        }

        /**
         * Sets the job ID supplier. If the deserialised ingest job does not have an ID, this supplier is used to
         * generate a new ID.
         *
         * @param  jobIdSupplier the job ID supplier
         * @return               the builder
         */
        public Builder<T> jobIdSupplier(Supplier<String> jobIdSupplier) {
            this.jobIdSupplier = jobIdSupplier;
            return this;
        }

        /**
         * Sets the time supplier.
         *
         * @param  timeSupplier the time supplier
         * @return              the builder
         */
        public Builder<T> timeSupplier(Supplier<Instant> timeSupplier) {
            this.timeSupplier = timeSupplier;
            return this;
        }

        public IngestJobMessageHandler<T> build() {
            return new IngestJobMessageHandler<>(this);
        }
    }
}

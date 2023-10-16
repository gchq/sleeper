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

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static sleeper.ingest.job.status.IngestJobValidatedEvent.ingestJobRejected;

public class IngestJobValidationUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestJobValidationUtils.class);

    private IngestJobValidationUtils() {
    }

    public static <T> Optional<T> deserialiseAndValidate(
            String message, Function<String, T> deserialiser, Function<T, List<String>> getValidationFailures,
            IngestJobStatusStore ingestJobStatusStore, Supplier<String> invalidJobIdSupplier, Supplier<Instant> timeSupplier) {
        T job;
        try {
            job = deserialiser.apply(message);
            LOGGER.info("Deserialised message to ingest job {}", job);
        } catch (RuntimeException e) {
            LOGGER.warn("Deserialisation failed: {}", e.getStackTrace());
            ingestJobStatusStore.jobValidated(
                    ingestJobRejected(invalidJobIdSupplier.get(), message, timeSupplier.get(),
                            "Error parsing JSON. Reason: " + Optional.ofNullable(e.getCause()).orElse(e).getMessage()));
            return Optional.empty();
        }
        List<String> validationFailures = getValidationFailures.apply(job);
        if (validationFailures.isEmpty()) {
            LOGGER.info("No validation failures found");
            return Optional.of(job);
        } else {
            LOGGER.warn("Validation failed: {}", validationFailures);
            ingestJobStatusStore.jobValidated(
                    ingestJobRejected(invalidJobIdSupplier.get(), message, timeSupplier.get(),
                            validationFailures.stream()
                                    .map(failure -> "Model validation failed. " + failure)
                                    .collect(Collectors.toList())));
            return Optional.empty();
        }
    }
}

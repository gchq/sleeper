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

package sleeper.build.uptime.lambda;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedList;
import java.util.Optional;

public class NightlyTestSummaryTable {
    private static final Logger LOGGER = LoggerFactory.getLogger(NightlyTestSummaryTable.class);

    private static final Gson GSON = createGson();

    private final LinkedList<Execution> executions = new LinkedList<>();

    private NightlyTestSummaryTable() {
    }

    public static NightlyTestSummaryTable empty() {
        return new NightlyTestSummaryTable();
    }

    public static NightlyTestSummaryTable fromJson(String json) {
        return GSON.fromJson(json, NightlyTestSummaryTable.class);
    }

    public static NightlyTestSummaryTable fromS3(GetS3ObjectAsString s3, String bucketName) {
        LOGGER.info("Loading existing test summary from S3");
        Optional<NightlyTestSummaryTable> summaryOpt = s3.getS3ObjectAsString(bucketName, "summary.json")
                .map(NightlyTestSummaryTable::fromJson);
        if (summaryOpt.isPresent()) {
            LOGGER.info("Found test summary with {} executions", summaryOpt.get().executions.size());
            return summaryOpt.get();
        } else {
            LOGGER.info("Found no test summary");
            return empty();
        }
    }

    public String toJson() {
        return GSON.toJson(this);
    }

    public boolean containsTestFromToday(Instant now) {
        Instant today = now.truncatedTo(ChronoUnit.DAYS);
        return executions.stream()
                .map(execution -> execution.startTime)
                .anyMatch(startTime -> startTime.truncatedTo(ChronoUnit.DAYS).equals(today));
    }

    public static class Execution {

        private final Instant startTime;

        public Execution(Instant startTime) {
            this.startTime = startTime;
        }
    }

    public static Gson createGson() {
        return new GsonBuilder()
                .registerTypeAdapter(Instant.class, new InstantSerDe())
                .create();
    }

    private static class InstantSerDe implements JsonSerializer<Instant>, JsonDeserializer<Instant> {
        @Override
        public Instant deserialize(JsonElement element, Type type, JsonDeserializationContext context) {
            return Instant.parse(element.getAsString());
        }

        @Override
        public JsonElement serialize(Instant instant, Type type, JsonSerializationContext context) {
            return new JsonPrimitive(instant.toString());
        }
    }
}

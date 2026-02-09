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

package sleeper.clients.report.ingest.batcher;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializer;

import sleeper.clients.util.ClientsGsonConfig;
import sleeper.core.table.TableStatus;
import sleeper.core.table.TableStatusProvider;
import sleeper.ingest.batcher.core.IngestBatcherTrackedFile;

import java.io.PrintStream;
import java.util.List;
import java.util.Optional;

/**
 * Creates reports in JSON format on the status of files tracked by the ingest batcher.
 */
public class JsonIngestBatcherReporter implements IngestBatcherReporter {
    private final PrintStream out;

    public JsonIngestBatcherReporter() {
        this(System.out);
    }

    public JsonIngestBatcherReporter(PrintStream out) {
        this.out = out;
    }

    @Override
    public void report(List<IngestBatcherTrackedFile> fileList, BatcherQuery.Type queryType, TableStatusProvider tableProvider) {
        Gson gson = createGson(tableProvider);
        JsonObject jsonObject = new JsonObject();
        jsonObject.add("fileList", gson.toJsonTree(fileList));
        out.println(gson.toJson(jsonObject));
    }

    private static Gson createGson(TableStatusProvider tableProvider) {
        return ClientsGsonConfig.standardBuilder()
                .registerTypeAdapter(IngestBatcherTrackedFile.class, fileSerializer(tableProvider))
                .create();
    }

    private static JsonSerializer<IngestBatcherTrackedFile> fileSerializer(TableStatusProvider tableProvider) {
        return (request, type, context) -> {
            JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("file", request.getFile());
            jsonObject.addProperty("fileSizeBytes", request.getFileSizeBytes());
            Optional<TableStatus> table = tableProvider.getById(request.getTableId());
            if (table.isPresent()) {
                jsonObject.addProperty("tableName", table.get().getTableName());
            } else {
                jsonObject.addProperty("tableId", request.getTableId());
                jsonObject.addProperty("tableExists", false);
            }
            jsonObject.add("receivedTime", context.serialize(request.getReceivedTime()));
            jsonObject.addProperty("jobId", request.getJobId());
            return jsonObject;
        };
    }
}

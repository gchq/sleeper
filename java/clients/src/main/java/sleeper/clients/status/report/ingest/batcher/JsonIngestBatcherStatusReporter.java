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

package sleeper.clients.status.report.ingest.batcher;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import sleeper.clients.util.GsonConfig;
import sleeper.ingest.batcher.FileIngestRequest;

import java.io.PrintStream;
import java.util.List;

public class JsonIngestBatcherStatusReporter implements IngestBatcherStatusReporter {
    private final Gson gson = GsonConfig.standardBuilder().create();
    private final PrintStream out;

    public JsonIngestBatcherStatusReporter(PrintStream out) {
        this.out = out;
    }

    @Override
    public void report(List<FileIngestRequest> fileList, BatcherQuery query) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.add("fileList", gson.toJsonTree(fileList));
        out.println(gson.toJson(jsonObject));
    }
}

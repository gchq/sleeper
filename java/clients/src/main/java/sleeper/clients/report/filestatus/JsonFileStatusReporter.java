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
package sleeper.clients.report.filestatus;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import sleeper.clients.util.ClientsGsonConfig;
import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.AllReferencesToAllFiles;
import sleeper.core.statestore.FileReference;

import java.io.PrintStream;

/**
 * Returns file status information in JSON format to the user on the console.
 */
public class JsonFileStatusReporter implements FileStatusReporter {

    private final Gson gson = ClientsGsonConfig.standardBuilder()
            .serializeSpecialFloatingPointValues()
            .registerTypeAdapter(AllReferencesToAllFiles.class, allFileReferencesJsonSerializer())
            .create();
    private final PrintStream out;

    public JsonFileStatusReporter() {
        this(System.out);
    }

    public JsonFileStatusReporter(PrintStream out) {
        this.out = out;
    }

    @Override
    public void report(TableFilesStatus status, boolean verbose) {
        out.println(gson.toJson(status));
    }

    private static JsonSerializer<AllReferencesToAllFiles> allFileReferencesJsonSerializer() {
        return (files, type, context) -> createAllFileReferencesJson(files, context);
    }

    private static JsonElement createAllFileReferencesJson(AllReferencesToAllFiles files, JsonSerializationContext context) {
        JsonArray filesArray = new JsonArray();
        for (AllReferencesToAFile file : files.getFiles()) {
            filesArray.add(createFileJson(file, context));
        }
        return filesArray;
    }

    private static JsonElement createFileJson(AllReferencesToAFile file, JsonSerializationContext context) {
        JsonObject fileObj = new JsonObject();
        fileObj.addProperty("filename", file.getFilename());
        fileObj.add("lastUpdateTime", context.serialize(file.getLastStateStoreUpdateTime()));
        fileObj.addProperty("totalReferenceCount", file.getReferenceCount());
        JsonArray referencesArr = new JsonArray();
        for (FileReference reference : file.getReferences()) {
            referencesArr.add(createFileReferenceJson(reference, context));
        }
        fileObj.add("internalReferences", referencesArr);
        return fileObj;
    }

    private static JsonElement createFileReferenceJson(FileReference file, JsonSerializationContext context) {
        JsonObject fileObj = new JsonObject();
        fileObj.addProperty("partitionId", file.getPartitionId());
        fileObj.addProperty("numberOfRows", file.getNumberOfRows());
        fileObj.addProperty("jobId", file.getJobId());
        fileObj.add("lastUpdateTime", context.serialize(file.getLastStateStoreUpdateTime()));
        fileObj.addProperty("countApproximate", file.isCountApproximate());
        fileObj.addProperty("onlyContainsDataForThisPartition", file.onlyContainsDataForThisPartition());
        return fileObj;
    }
}

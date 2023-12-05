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
package sleeper.clients.status.report.filestatus;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.PrintStream;

/**
 * An implementation that returns {@link FileStatus} information in JSON format
 * to a user via the console.
 */
public class JsonFileStatusReporter implements FileStatusReporter {

    private final Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues()
            .create();
    private final PrintStream out;

    public JsonFileStatusReporter() {
        this(System.out);
    }

    public JsonFileStatusReporter(PrintStream out) {
        this.out = out;
    }

    @Override
    public void report(FileStatus fileStatusReport, boolean verbose) {
        out.println(gson.toJson(fileStatusReport));
    }
}

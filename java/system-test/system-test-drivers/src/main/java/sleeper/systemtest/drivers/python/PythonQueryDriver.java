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

package sleeper.systemtest.drivers.python;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import sleeper.systemtest.drivers.instance.SleeperInstanceContext;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static sleeper.configuration.properties.instance.CommonProperty.ID;

public class PythonQueryDriver {
    private static final Gson GSON = new GsonBuilder().create();
    private final SleeperInstanceContext instance;
    private final PythonRunner pythonRunner;
    private final Path pythonDir;

    public PythonQueryDriver(SleeperInstanceContext instance, Path pythonDir) {
        this.instance = instance;
        this.pythonRunner = new PythonRunner(pythonDir);
        this.pythonDir = pythonDir;
    }

    public void exact(String queryId, Map<String, List<Object>> queryList) throws IOException, InterruptedException {
        pythonRunner.run(
                pythonDir.resolve("test/exact_query.py").toString(),
                "--instance", instance.getInstanceProperties().get(ID),
                "--table", instance.getTableName(),
                "--queryid", queryId,
                "--query", GSON.toJson(queryList));
    }
}

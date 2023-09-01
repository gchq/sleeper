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

import sleeper.clients.util.ClientUtils;
import sleeper.clients.util.RunCommand;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;

import java.io.IOException;
import java.nio.file.Path;

import static sleeper.configuration.properties.instance.CommonProperty.ID;

public class PythonIngestDriver {
    private final SleeperInstanceContext instance;
    private final RunCommand commandRunner = ClientUtils::runCommandInheritIO;
    private final Path pythonDir;

    public PythonIngestDriver(SleeperInstanceContext instance, Path pythonDir) {
        this.instance = instance;
        this.pythonDir = pythonDir;
    }

    public void direct(Path file) throws IOException, InterruptedException {
        commandRunner.run("python3",
                pythonDir.resolve("test/batch_writer.py").toString(),
                "--instance", instance.getInstanceProperties().get(ID),
                "--table", instance.getTableName(),
                "--files", file.toString());
    }
}

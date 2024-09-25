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

package sleeper.systemtest.drivers.python;

import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.ingest.IngestLocalFileByAnyQueueDriver;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.nio.file.Path;

import static sleeper.core.properties.instance.CommonProperty.ID;

public class PythonIngestLocalFileDriver implements IngestLocalFileByAnyQueueDriver {
    private final SystemTestInstanceContext instance;
    private final PythonRunner pythonRunner;
    private final Path pythonDir;

    public PythonIngestLocalFileDriver(SystemTestInstanceContext instance, Path pythonDir, SystemTestClients clients) {
        this.instance = instance;
        this.pythonRunner = new PythonRunner(pythonDir, clients);
        this.pythonDir = pythonDir;
    }

    public void uploadLocalFileAndSendJob(Path tempDir, String jobId, String file) {
        pythonRunner.run(
                pythonDir.resolve("test/batch_writer.py").toString(),
                "--instance", instance.getInstanceProperties().get(ID),
                "--table", instance.getTableName(),
                "--jobid", jobId,
                "--file", tempDir.resolve(file).toString());
    }

}

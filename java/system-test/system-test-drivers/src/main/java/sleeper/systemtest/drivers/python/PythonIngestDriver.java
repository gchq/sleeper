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
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.ingest.IngestByAnyQueueDriver;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.sourcedata.IngestSourceFilesContext;

import java.nio.file.Path;
import java.util.stream.Stream;

import static sleeper.core.properties.instance.CommonProperty.ID;

public class PythonIngestDriver implements IngestByAnyQueueDriver {
    private final SystemTestInstanceContext instance;
    private final IngestSourceFilesContext sourceFiles;
    private final PythonRunner pythonRunner;
    private final Path pythonDir;

    public PythonIngestDriver(SystemTestContext context, SystemTestClients clients) {
        this.instance = context.instance();
        this.sourceFiles = context.sourceFiles();
        this.pythonDir = context.parameters().getPythonDirectory();
        this.pythonRunner = new PythonRunner(pythonDir, clients);
    }

    public void sendJobWithFiles(String jobId, String... files) {
        pythonRunner.run(Stream.concat(
                Stream.of(pythonDir.resolve("test/ingest_files_from_s3.py").toString(),
                        "--instance", instance.getInstanceProperties().get(ID),
                        "--table", instance.getTableName(),
                        "--jobid", jobId,
                        "--files"),
                Stream.of(files).map(sourceFiles::ingestJobFileInBucket))
                .toArray(String[]::new));
    }
}

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
import sleeper.clients.util.CommandPipeline;
import sleeper.clients.util.CommandPipelineRunner;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;

import java.io.IOException;
import java.nio.file.Path;
import java.util.stream.Stream;

import static sleeper.clients.util.Command.command;
import static sleeper.clients.util.CommandPipeline.pipeline;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_SOURCE_BUCKET;

public class PythonBulkImportDriver {
    private final SleeperInstanceContext instance;
    private final CommandPipelineRunner pipelineRunner = ClientUtils::runCommandInheritIO;
    private final Path pythonDir;

    public PythonBulkImportDriver(SleeperInstanceContext instance, Path pythonDir) {
        this.instance = instance;
        this.pythonDir = pythonDir;
    }

    public void emrServerless(String jobId, String... files) throws IOException, InterruptedException {
        runInVenv(pipeline(command(Stream.concat(
                        Stream.of("python3",
                                pythonDir.resolve("test/bulk_import_files_from_s3.py").toString(),
                                "--instance", instance.getInstanceProperties().get(ID),
                                "--table", instance.getTableName(),
                                "--platform", "EMRServerless",
                                "--jobid", jobId,
                                "--files"),
                        Stream.of(files)
                                .map(file -> instance.getInstanceProperties().get(INGEST_SOURCE_BUCKET) + "/" + file))
                .toArray(String[]::new))));
    }

    public void runInVenv(CommandPipeline pipeline) throws IOException, InterruptedException {
        pipelineRunner.run(activateVenv(pythonDir));
        pipelineRunner.run(pipeline);
        pipelineRunner.run(deactivateVenv());
    }

    public static CommandPipeline activateVenv(Path pythonDir) {
        return pipeline(command("source", pythonDir.resolve("env/bin/activate").toString()));
    }

    public static CommandPipeline deactivateVenv() {
        return pipeline(command("deactivate"));
    }
}

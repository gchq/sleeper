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

import sleeper.systemtest.dsl.ingest.IngestByAnyQueueDriver;
import sleeper.systemtest.dsl.ingest.IngestFromLocalFileDriver;
import sleeper.systemtest.dsl.instance.SleeperInstanceContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.stream.Stream;

import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_SOURCE_BUCKET;

public class PythonIngestDriver implements IngestFromLocalFileDriver, IngestByAnyQueueDriver {
    private final SleeperInstanceContext instance;
    private final PythonRunner pythonRunner;
    private final Path pythonDir;

    public PythonIngestDriver(SleeperInstanceContext instance, Path pythonDir) {
        this.instance = instance;
        this.pythonRunner = new PythonRunner(pythonDir);
        this.pythonDir = pythonDir;
    }

    public void uploadAndSendJob(Path tempDir, String jobId, String file) {
        try {
            pythonRunner.run(
                    pythonDir.resolve("test/batch_writer.py").toString(),
                    "--instance", instance.getInstanceProperties().get(ID),
                    "--table", instance.getTableName(),
                    "--jobid", jobId,
                    "--file", tempDir.resolve(file).toString());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public void sendJobWithFiles(String jobId, String... files) {
        try {
            pythonRunner.run(Stream.concat(
                            Stream.of(pythonDir.resolve("test/ingest_files_from_s3.py").toString(),
                                    "--instance", instance.getInstanceProperties().get(ID),
                                    "--table", instance.getTableName(),
                                    "--jobid", jobId,
                                    "--files"),
                            Stream.of(files)
                                    .map(file -> instance.getInstanceProperties().get(INGEST_SOURCE_BUCKET) + "/" + file))
                    .toArray(String[]::new));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}

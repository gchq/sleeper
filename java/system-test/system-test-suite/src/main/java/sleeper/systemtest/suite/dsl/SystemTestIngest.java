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

package sleeper.systemtest.suite.dsl;

import sleeper.systemtest.drivers.ingest.DirectIngestDriver;
import sleeper.systemtest.drivers.ingest.IngestBatcherDriver;
import sleeper.systemtest.drivers.ingest.IngestByQueueDriver;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;
import sleeper.systemtest.drivers.instance.SystemTestParameters;

import java.nio.file.Path;

public class SystemTestIngest {

    private final SystemTestParameters parameters;
    private final SleeperInstanceContext instance;
    private final SystemTestClients clients;

    public SystemTestIngest(SystemTestParameters parameters,
                            SleeperInstanceContext instance,
                            SystemTestClients clients) {
        this.parameters = parameters;
        this.instance = instance;
        this.clients = clients;
    }

    public SystemTestIngestBatcher batcher() {
        return new SystemTestIngestBatcher(this, parameters, instance,
                new IngestBatcherDriver(instance, clients.getDynamoDB(), clients.getSqs(), clients.getLambda()));
    }

    public SystemTestDirectIngest direct(Path tempDir) {
        return new SystemTestDirectIngest(new DirectIngestDriver(instance, tempDir));
    }

    IngestByQueueDriver byQueueDriver() {
        return new IngestByQueueDriver(instance, clients.getDynamoDB(), clients.getLambda());
    }
}

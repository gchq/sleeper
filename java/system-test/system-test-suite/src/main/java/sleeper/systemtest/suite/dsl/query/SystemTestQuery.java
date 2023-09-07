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

package sleeper.systemtest.suite.dsl.query;

import sleeper.systemtest.drivers.instance.SleeperInstanceContext;
import sleeper.systemtest.drivers.query.DirectQueryDriver;
import sleeper.systemtest.drivers.query.S3ResultsDriver;
import sleeper.systemtest.suite.fixtures.SystemTestClients;

public class SystemTestQuery {
    private final SleeperInstanceContext instance;
    private final SystemTestClients clients;

    public SystemTestQuery(SleeperInstanceContext instance, SystemTestClients clients) {
        this.instance = instance;
        this.clients = clients;
    }

    public SystemTestSQSQuery byQueue() {
        return new SystemTestSQSQuery(instance, clients);
    }

    public SystemTestDirectQuery direct() {
        return new SystemTestDirectQuery(new DirectQueryDriver(instance));
    }

    public void emptyResultsBucket() {
        new S3ResultsDriver(instance, clients.getS3()).emptyBucket();
    }
}

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

package sleeper.systemtest.suite.dsl.python;

import sleeper.core.record.Record;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;
import sleeper.systemtest.drivers.python.PythonQueryDriver;
import sleeper.systemtest.drivers.query.S3ResultsDriver;
import sleeper.systemtest.drivers.query.WaitForResultsDriver;
import sleeper.systemtest.suite.fixtures.SystemTestClients;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

public class SystemTestPythonQuery {
    private final PythonQueryDriver pythonQueryDriver;
    private final S3ResultsDriver s3ResultsDriver;
    private final WaitForResultsDriver waitForResultsDriver;
    private final List<String> queryIds = new ArrayList<>();

    public SystemTestPythonQuery(SleeperInstanceContext instance, SystemTestClients clients, Path pythonDir) {
        this.pythonQueryDriver = new PythonQueryDriver(instance, pythonDir);
        this.s3ResultsDriver = new S3ResultsDriver(instance, clients.getS3());
        this.waitForResultsDriver = new WaitForResultsDriver(instance, clients.getS3());
    }

    public SystemTestPythonQuery exact(Map<String, List<Object>> queryMap) throws IOException, InterruptedException {
        String queryId = UUID.randomUUID().toString();
        pythonQueryDriver.exact(queryId, queryMap);
        queryIds.add(queryId);
        return this;
    }

    public Stream<Record> results() throws InterruptedException {
        waitForResultsDriver.waitForResults(queryIds);
        return queryIds.stream().flatMap(s3ResultsDriver::results);
    }
}

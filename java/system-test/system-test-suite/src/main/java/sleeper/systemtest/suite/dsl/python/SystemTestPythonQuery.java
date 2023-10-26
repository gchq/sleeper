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

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

public class SystemTestPythonQuery {
    private final PythonQueryDriver pythonQueryDriver;
    private final List<String> queryIds = new ArrayList<>();

    public SystemTestPythonQuery(SleeperInstanceContext instance, Path pythonDir, Path outputDir) {
        this.pythonQueryDriver = new PythonQueryDriver(instance, pythonDir, outputDir);
    }

    public SystemTestPythonQuery exactKeys(String keyName, String... keyValues) throws IOException, InterruptedException {
        String queryId = UUID.randomUUID().toString();
        pythonQueryDriver.exactKeys(queryId, keyName, List.of(keyValues));
        queryIds.add(queryId);
        return this;
    }

    public SystemTestPythonQuery range(String key, Object min, Object max) throws IOException, InterruptedException {
        String queryId = UUID.randomUUID().toString();
        pythonQueryDriver.range(queryId, key, min, max);
        queryIds.add(queryId);
        return this;
    }

    public SystemTestPythonQuery range(String key, String table, Object min, Object max)
            throws IOException, InterruptedException {
        String queryId = UUID.randomUUID().toString();
        pythonQueryDriver.range(queryId, key, table, min, max);
        queryIds.add(queryId);
        return this;
    }

    public SystemTestPythonQuery range(String key, Object min, boolean minInclusive, Object max, boolean maxInclusive)
            throws IOException, InterruptedException {
        String queryId = UUID.randomUUID().toString();
        pythonQueryDriver.range(queryId, key, min, minInclusive, max, maxInclusive);
        queryIds.add(queryId);
        return this;
    }

    public Stream<Record> results() {
        return queryIds.stream().flatMap(pythonQueryDriver::results);
    }
}

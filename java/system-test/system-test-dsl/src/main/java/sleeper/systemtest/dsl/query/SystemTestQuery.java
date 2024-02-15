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

package sleeper.systemtest.dsl.query;

import sleeper.core.record.Record;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.util.List;
import java.util.Map;

public class SystemTestQuery {
    private final SystemTestInstanceContext instance;
    private final QueryAllTablesDriver byQueueDriver;
    private final QueryAllTablesDriver directDriver;
    private final ClearQueryResultsDriver clearResultsDriver;
    private QueryAllTablesDriver driver = null;

    public SystemTestQuery(SystemTestInstanceContext instance,
                           QueryAllTablesDriver byQueueDriver,
                           QueryAllTablesDriver directDriver,
                           ClearQueryResultsDriver clearResultsDriver) {
        this.instance = instance;
        this.byQueueDriver = byQueueDriver;
        this.directDriver = directDriver;
        this.clearResultsDriver = clearResultsDriver;
    }

    public SystemTestQuery byQueue() {
        driver = byQueueDriver;
        return this;
    }

    public SystemTestQuery direct() {
        driver = directDriver;
        return this;
    }

    public List<Record> allRecordsInTable() {
        return driver.run(queryCreator().allRecordsQuery());
    }

    public Map<String, List<Record>> allRecordsByTable() {
        return driver.runForAllTables(QueryCreator::allRecordsQuery);
    }

    public List<Record> byRowKey(String key, QueryRange... ranges) {
        return driver.run(queryCreator().byRowKey(key, List.of(ranges)));
    }

    public void emptyResultsBucket() {
        clearResultsDriver.deleteAllQueryResults();
    }

    private QueryCreator queryCreator() {
        return new QueryCreator(instance);
    }
}

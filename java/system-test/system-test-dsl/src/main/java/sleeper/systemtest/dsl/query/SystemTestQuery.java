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
import sleeper.query.core.model.Query;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.SystemTestDrivers;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class SystemTestQuery {
    private final SystemTestContext context;
    private final SystemTestDrivers baseDrivers;
    private final SystemTestDrivers adminDrivers;
    private QueryAllTablesDriver driver = null;

    public SystemTestQuery(SystemTestContext context, SystemTestDrivers baseDrivers) {
        this.context = context;
        this.baseDrivers = baseDrivers;
        this.adminDrivers = context.instance().adminDrivers();
    }

    public SystemTestQuery byQueue() {
        driver = adminDrivers.queryByQueue(context);
        return this;
    }

    public SystemTestQuery direct() {
        driver = adminDrivers.directQuery(context);
        return this;
    }

    public SystemTestQuery webSocket() {
        // Note that this relies on permissions of the base credentials,
        // as the instance admin does not currently have working permissions for the web socket.
        // TODO add the correct permissions to the instance admin role
        driver = baseDrivers.queryByWebSocket(context);
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

    public List<Record> withConfiguration(Consumer<Query.Builder> config) {
        Query.Builder builder = queryCreator().allRecordsBuilder();
        config.accept(builder);
        return driver.run(builder.build());
    }

    public void emptyResultsBucket() {
        adminDrivers.clearQueryResults(context).deleteAllQueryResults();
    }

    private QueryCreator queryCreator() {
        return new QueryCreator(context.instance());
    }
}

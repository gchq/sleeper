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

package sleeper.systemtest.drivers.query;

import sleeper.core.range.Range;
import sleeper.core.range.Region;
import sleeper.core.schema.Schema;
import sleeper.query.model.Query;
import sleeper.query.model.QuerySerDe;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;

import java.util.Map;
import java.util.UUID;

public class QueryCreator {
    private final Schema schema;
    private final String tableName;
    private final QuerySerDe querySerDe;

    public QueryCreator(SleeperInstanceContext instance) {
        this.schema = instance.getTableProperties().getSchema();
        this.tableName = instance.getTableName();
        this.querySerDe = new QuerySerDe(Map.of(tableName, schema));
    }

    public Query create(String key, Object min, Object max) {
        return create(UUID.randomUUID().toString(), key, min, max);
    }

    public Query create(String queryId, String key, Object min, Object max) {
        Range.RangeFactory rangeFactory = new Range.RangeFactory(schema);
        Region region = new Region(rangeFactory.createRange(key, min, max));
        return new Query.Builder(tableName, queryId, region).build();
    }

    public String asString(String queryId, String key, Object min, Object max) {
        return querySerDe.toJson(create(queryId, key, min, max));
    }
}

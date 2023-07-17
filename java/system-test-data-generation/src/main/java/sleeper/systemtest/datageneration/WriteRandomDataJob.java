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
package sleeper.systemtest.datageneration;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.statestore.StateStoreException;
import sleeper.systemtest.configuration.SystemTestProperties;

import java.io.IOException;
import java.util.Iterator;
import java.util.stream.Stream;

import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_RECORDS_PER_WRITER;

public abstract class WriteRandomDataJob {
    private final SystemTestProperties systemTestProperties;
    private final TableProperties tableProperties;

    public WriteRandomDataJob(SystemTestProperties systemTestProperties,
                              TableProperties tableProperties) {
        this.systemTestProperties = systemTestProperties;
        this.tableProperties = tableProperties;
    }

    public abstract void run() throws IOException, StateStoreException;

    protected Iterator<Record> createRecordIterator(Schema schema) {
        RandomRecordSupplierConfig config = new RandomRecordSupplierConfig(systemTestProperties);
        return Stream
                .generate(new RandomRecordSupplier(schema, config))
                .limit(systemTestProperties.getLong(NUMBER_OF_RECORDS_PER_WRITER))
                .iterator();
    }

    protected SystemTestProperties getSystemTestProperties() {
        return systemTestProperties;
    }

    protected TableProperties getTableProperties() {
        return tableProperties;
    }
}

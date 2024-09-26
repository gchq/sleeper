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
package sleeper.systemtest.datageneration;

import sleeper.core.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.systemtest.configuration.SystemTestProperties;
import sleeper.systemtest.configuration.SystemTestPropertyValues;

import java.util.Iterator;
import java.util.stream.Stream;

import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_RECORDS_PER_INGEST;

public class WriteRandomData {

    private WriteRandomData() {
    }

    public static Iterator<Record> createRecordIterator(
            SystemTestProperties systemTestProperties, TableProperties tableProperties) {
        return createRecordIterator(systemTestProperties.testPropertiesOnly(), tableProperties);
    }

    public static Iterator<Record> createRecordIterator(
            SystemTestPropertyValues systemTestProperties, TableProperties tableProperties) {
        RandomRecordSupplierConfig config = new RandomRecordSupplierConfig(systemTestProperties);
        return Stream
                .generate(new RandomRecordSupplier(tableProperties.getSchema(), config))
                .limit(systemTestProperties.getLong(NUMBER_OF_RECORDS_PER_INGEST))
                .iterator();
    }
}

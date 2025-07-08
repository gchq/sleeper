/*
 * Copyright 2022-2025 Crown Copyright
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
import sleeper.core.record.SleeperRow;
import sleeper.systemtest.configuration.SystemTestDataGenerationJob;

import java.util.Iterator;
import java.util.stream.Stream;

public class WriteRandomData {

    private WriteRandomData() {
    }

    public static Iterator<SleeperRow> createRecordIterator(
            SystemTestDataGenerationJob job, TableProperties tableProperties) {
        return Stream
                .generate(new RandomRecordSupplier(tableProperties.getSchema(), job.getRandomDataSettings()))
                .limit(job.getRecordsPerIngest())
                .iterator();
    }
}

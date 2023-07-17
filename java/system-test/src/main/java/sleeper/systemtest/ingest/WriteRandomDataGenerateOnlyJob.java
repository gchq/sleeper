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
package sleeper.systemtest.ingest;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.statestore.StateStoreProvider;
import sleeper.systemtest.configuration.SystemTestProperties;

import java.io.IOException;

public class WriteRandomDataGenerateOnlyJob extends WriteRandomDataJob {
    public WriteRandomDataGenerateOnlyJob(
            ObjectFactory objectFactory,
            SystemTestProperties systemTestProperties,
            TableProperties tableProperties,
            StateStoreProvider stateStoreProvider) {
        super(objectFactory, systemTestProperties, tableProperties, stateStoreProvider);
    }

    @Override
    public void run() throws IOException {
        WriteRandomDataFiles.writeToS3GetDirectory(
                getSystemTestProperties(), getTableProperties(),
                createRecordIterator(getTableProperties().getSchema()));
    }
}

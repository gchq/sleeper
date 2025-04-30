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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.systemtest.configuration.SystemTestStandaloneProperties;

import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

public class SystemTestTaskIT extends LocalStackTestBase {

    InstanceProperties instanceProperties = createTestInstanceProperties();
    SystemTestStandaloneProperties systemTestProperties = new SystemTestStandaloneProperties();

    @Disabled("TODO")
    @Test
    void shouldIngestDirectly() throws Exception {
        IngestRandomData ingest = new IngestRandomData(instanceProperties, systemTestProperties, "system-test", stsClient, stsClientV2, hadoopConf);
        ingest.run();
    }

}

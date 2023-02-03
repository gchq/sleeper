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

package sleeper.configuration;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;

public class InstanceConfiguration {
    private final AmazonS3 s3;
    private final InstanceProperties instanceProperties;
    private final List<TableProperties> tables = new ArrayList<>();

    private InstanceConfiguration(AmazonS3 s3, InstanceProperties instanceProperties) throws IOException {
        this.s3 = s3;
        this.instanceProperties = instanceProperties;
        loadTablesFromS3();
    }

    public static InstanceConfiguration loadFromS3(AmazonS3 s3, String instanceId) throws IOException {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3GivenInstanceId(s3, instanceId);
        return new InstanceConfiguration(s3, instanceProperties);
    }

    private void loadTablesFromS3() throws IOException {
        String configBucket = getConfigBucket();
        for (S3ObjectSummary tableConfigObject : s3
                .listObjectsV2(configBucket, "tables/")
                .getObjectSummaries()) {

            String key = tableConfigObject.getKey();
            TableProperties tableProperties = new TableProperties(instanceProperties);
            try (InputStream in = s3.getObject(configBucket, key).getObjectContent()) {
                tableProperties.load(in);
            }
            tables.add(tableProperties);
        }
    }

    public InstanceProperties getInstanceProperties() {
        return instanceProperties;
    }

    public String getConfigBucket() {
        return instanceProperties.get(CONFIG_BUCKET);
    }

    public List<TableProperties> getTables() {
        return tables;
    }
}

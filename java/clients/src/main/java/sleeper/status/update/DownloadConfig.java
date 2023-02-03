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
package sleeper.status.update;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;

public class DownloadConfig {

    private DownloadConfig() {
    }

    public static void main(String[] args) throws IOException {
        if (1 != args.length) {
            throw new IllegalArgumentException("Usage: <instance id>");
        }
        String instanceId = args[0];

        AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3GivenInstanceId(s3, instanceId);
        instanceProperties.save(Path.of("./instance.properties"));

        String configBucket = instanceProperties.get(CONFIG_BUCKET);
        for (S3ObjectSummary tableConfigObject : s3
                .listObjectsV2(configBucket, "tables/")
                .getObjectSummaries()) {

            String key = tableConfigObject.getKey();
            TableProperties tableProperties = new TableProperties(instanceProperties);
            try (InputStream in = s3.getObject(configBucket, key).getObjectContent()) {
                tableProperties.load(in);
            }

            Path tableFolder = Path.of("./", key);
            Files.createDirectories(tableFolder);
            tableProperties.save(tableFolder.resolve("table.properties"));
            tableProperties.getSchema().save(tableFolder.resolve("schema.json"));
            Files.writeString(tableFolder.resolve("tableBucket.txt"), tableProperties.get(DATA_BUCKET));
        }
    }
}

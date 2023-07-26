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

package sleeper.clients.docker;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.iterable.S3Objects;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;

import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;

public class TearDownDockerInstance {
    private TearDownDockerInstance() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("Usage: <instance-id>");
        }
        if (System.getenv("AWS_ENDPOINT_URL") == null) {
            throw new IllegalArgumentException("Environment variable AWS_ENDPOINT_URL not set");
        }
        String instanceId = args[0];
        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamoDB = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());

        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3(s3Client, InstanceProperties.getConfigBucketFromInstanceId(instanceId));
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.loadFromS3(s3Client, "system-test");

        tearDownBucket(s3Client, instanceProperties.get(CONFIG_BUCKET));
        tearDownBucket(s3Client, tableProperties.get(DATA_BUCKET));
        dynamoDB.listTables().getTableNames()
                .stream().filter(table -> table.startsWith("sleeper-" + instanceId))
                .forEach(dynamoDB::deleteTable);
    }

    public static void tearDownBucket(AmazonS3 s3Client, String bucketName) {
        S3Objects.inBucket(s3Client, bucketName)
                .forEach(object -> s3Client.deleteObject(bucketName, object.getKey()));
        s3Client.deleteBucket(bucketName);
    }
}

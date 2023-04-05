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
package sleeper.bulkimport.job.runner;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityResult;
import com.google.gson.JsonSyntaxException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.bulkimport.job.BulkImportJob;
import sleeper.bulkimport.job.BulkImportJobSerDe;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreProvider;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFileAttributes;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_BUCKET;

/**
 * This abstract class executes a Spark job that reads in input Parquet files and writes
 * out files of {@link sleeper.core.record.Record}s. Concrete subclasses of this class must implement
 * a method which takes in a {@link Dataset} of {@link Row}s where a field has
 * been added that contains the Sleeper partition that the row is in and writes
 * the data to files in S3 and returns a list of the {@link FileInfo}s that
 * will then be used to update the {@link StateStore}.
 */
public class BulkImportJobRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkImportJobRunner.class);

    private final SparkRecordPartitioner partitioner;
    private InstanceProperties instanceProperties;
    private TablePropertiesProvider tablePropertiesProvider;
    private StateStoreProvider stateStoreProvider;

    public BulkImportJobRunner(SparkRecordPartitioner partitioner) {
        this.partitioner = partitioner;
    }

    public void init(InstanceProperties instanceProperties, AmazonS3 s3Client, AmazonDynamoDB dynamoClient) {
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = new TablePropertiesProvider(s3Client, instanceProperties);
        this.stateStoreProvider = new StateStoreProvider(dynamoClient, instanceProperties);
    }

    public void run(BulkImportJob job) throws IOException {
        new SparkBulkImportDriver(partitioner, instanceProperties, tablePropertiesProvider, stateStoreProvider)
                .run(job);
    }

    public static void start(String[] args, SparkRecordPartitioner partitioner) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("Expected two arguments, the first with the id of the bulk import job," +
                    " the second with the config bucket");
        }

        InstanceProperties instanceProperties = new InstanceProperties();
        AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();

        try {
            instanceProperties.loadFromS3(amazonS3, args[1]);
        } catch (Exception e) {
            // This is a good indicator if something is wrong with the permissions
            LOGGER.error("Failed to load instance properties", e);
            LOGGER.info("Checking whether token is readable");
            String token = System.getenv("AWS_WEB_IDENTITY_TOKEN_FILE");
            java.nio.file.Path tokenPath = Paths.get(token);
            boolean readable = Files.isReadable(tokenPath);
            LOGGER.info("Token was{} readable", readable ? "" : " not");
            if (!readable) {
                PosixFileAttributes readAttributes = Files.readAttributes(tokenPath, PosixFileAttributes.class);
                LOGGER.info("Token Permissions: {}", readAttributes.permissions());
                LOGGER.info("Token owner: {}", readAttributes.owner());
            }
            // This could error if not logged in correctly
            AWSSecurityTokenService sts = AWSSecurityTokenServiceClientBuilder.defaultClient();
            GetCallerIdentityResult callerIdentity = sts.getCallerIdentity(new GetCallerIdentityRequest());
            LOGGER.info("Logged in as: {}", callerIdentity.getArn());

            throw e;
        }

        String bulkImportBucket = instanceProperties.get(BULK_IMPORT_BUCKET);
        if (null == bulkImportBucket) {
            throw new RuntimeException("sleeper.bulk.import.bucket was not set. Has one of the bulk import stacks been deployed?");
        }
        String jsonJobKey = "bulk_import/" + args[0] + ".json";
        LOGGER.info("Loading bulk import job from key {} in bulk import bucket {}", bulkImportBucket, jsonJobKey);
        String jsonJob = amazonS3.getObjectAsString(bulkImportBucket, jsonJobKey);
        BulkImportJob bulkImportJob;
        try {
            bulkImportJob = new BulkImportJobSerDe().fromJson(jsonJob);
        } catch (JsonSyntaxException e) {
            LOGGER.error("Json job was malformed: {}", args[0]);
            throw e;
        }

        BulkImportJobRunner runner = new BulkImportJobRunner(partitioner);
        runner.init(instanceProperties, amazonS3, AmazonDynamoDBClientBuilder.defaultClient());
        runner.run(bulkImportJob);
    }
}

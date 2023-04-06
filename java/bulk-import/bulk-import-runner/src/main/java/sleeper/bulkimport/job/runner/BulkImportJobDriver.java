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
import com.joom.spark.ExplicitRepartitionStrategy$;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.SparkStrategy;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import sleeper.bulkimport.job.BulkImportJob;
import sleeper.bulkimport.job.BulkImportJobSerDe;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.partition.Partition;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.schema.Schema;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.status.store.job.IngestJobStatusStoreFactory;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.StateStoreProvider;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFileAttributes;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.FILE_SYSTEM;

/**
 * This abstract class executes a Spark job that reads in input Parquet files and writes
 * out files of {@link sleeper.core.record.Record}s. Concrete subclasses of this class must implement
 * a method which takes in a {@link Dataset} of {@link Row}s where a field has
 * been added that contains the Sleeper partition that the row is in and writes
 * the data to files in S3 and returns a list of the {@link FileInfo}s that
 * will then be used to update the {@link StateStore}.
 */
public class BulkImportJobDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkImportJobDriver.class);

    private final BulkImportJobRunner partitioner;
    private final InstanceProperties instanceProperties;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final IngestJobStatusStore statusStore;
    private final Supplier<Instant> getTime;

    public BulkImportJobDriver(BulkImportJobRunner partitioner, InstanceProperties instanceProperties,
                               AmazonS3 s3Client, AmazonDynamoDB dynamoClient) {
        this(partitioner, instanceProperties, s3Client, dynamoClient,
                IngestJobStatusStoreFactory.getStatusStore(dynamoClient, instanceProperties), Instant::now);
    }

    public BulkImportJobDriver(BulkImportJobRunner partitioner, InstanceProperties instanceProperties,
                               AmazonS3 s3Client, AmazonDynamoDB dynamoClient,
                               IngestJobStatusStore statusStore,
                               Supplier<Instant> getTime) {
        this.partitioner = partitioner;
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = new TablePropertiesProvider(s3Client, instanceProperties);
        this.stateStoreProvider = new StateStoreProvider(dynamoClient, instanceProperties);
        this.statusStore = statusStore;
        this.getTime = getTime;
    }

    public void run(BulkImportJob job, String taskId) throws IOException {
        Instant startTime = getTime.get();
        LOGGER.info("Received bulk import job with id {} at time {}", job.getId(), startTime);
        LOGGER.info("Job is {}", job);
        statusStore.jobStarted(taskId, job.toIngestJob(), startTime);

        // Initialise Spark
        LOGGER.info("Initialising Spark");
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.serializer", KryoSerializer.class.getName());
        sparkConf.registerKryoClasses(new Class[]{Partition.class});
        SparkSession session = new SparkSession.Builder().config(sparkConf).getOrCreate();
        scala.collection.immutable.List<SparkStrategy> strategies = JavaConverters.collectionAsScalaIterable(Collections.singletonList((org.apache.spark.sql.execution.SparkStrategy) ExplicitRepartitionStrategy$.MODULE$)).toList();
        session.experimental().extraStrategies_$eq(strategies);
        SparkContext sparkContext = session.sparkContext();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkContext);
        LOGGER.info("Spark initialised");

        // Load table information
        LOGGER.info("Loading table properties and schema for table {}", job.getTableName());
        TableProperties tableProperties = tablePropertiesProvider.getTableProperties(job.getTableName());
        Schema schema = tableProperties.getSchema();
        StructType convertedSchema = new StructTypeFactory().getStructType(schema);

        // Load statestore and partitions
        LOGGER.info("Loading statestore and partitions");
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        List<Partition> allPartitions;
        try {
            allPartitions = stateStore.getAllPartitions();
        } catch (StateStoreException e) {
            throw new RuntimeException("Failed to load statestore. Are permissions correct for this service account?");
        }

        Configuration conf = sparkContext.hadoopConfiguration();
        Broadcast<List<Partition>> broadcastedPartitions = javaSparkContext.broadcast(allPartitions);
        LOGGER.info("Starting data processing");

        // Create paths to be read
        List<String> pathsWithFs = new ArrayList<>();
        String fs = instanceProperties.get(FILE_SYSTEM);
        LOGGER.info("Using file system {}", fs);
        job.getFiles().forEach(file -> pathsWithFs.add(fs + file));
        LOGGER.info("Paths to be read are {}", pathsWithFs);

        // Run bulk import
        Dataset<Row> dataWithPartition = session.read()
                .schema(convertedSchema)
                .option("pathGlobFilter", "*.parquet")
                .option("recursiveFileLookup", "true")
                .parquet(pathsWithFs.toArray(new String[0]));

        LOGGER.info("Running bulk import job with id {}", job.getId());
        List<FileInfo> fileInfos = partitioner.createFileInfos(
                        SparkPartitionRequest.builder().rows(dataWithPartition)
                                .instanceProperties(instanceProperties).tableProperties(tableProperties)
                                .broadcastedPartitions(broadcastedPartitions).conf(conf).build())
                .collectAsList().stream()
                .map(SparkFileInfoRow::createFileInfo)
                .collect(Collectors.toList());

        long numRecords = fileInfos.stream()
                .mapToLong(FileInfo::getNumberOfRecords)
                .sum();
        try {
            stateStore.addFiles(fileInfos);
        } catch (StateStoreException e) {
            throw new RuntimeException("Failed to add files to state store. Ensure this service account has write access. Files may need to "
                    + "be re-imported for clients to accesss data");
        }
        LOGGER.info("Added {} files to statestore", fileInfos.size());
        Instant finishTime = getTime.get();
        LOGGER.info("Finished bulk import job {} at time {}", job.getId(), finishTime);
        long durationInSeconds = Duration.between(startTime, finishTime).getSeconds();
        double rate = numRecords / (double) durationInSeconds;
        LOGGER.info("Bulk import job {} took {} seconds (rate of {} per second)", job.getId(), durationInSeconds, rate);
        statusStore.jobFinished(taskId, job.toIngestJob(), new RecordsProcessedSummary(
                new RecordsProcessed(numRecords, numRecords), startTime, finishTime));

        sparkContext.stop(); // Calling this manually stops it potentially timing out after 10 seconds.
    }

    public static void start(String[] args, BulkImportJobRunner partitioner) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException("Expected 3 arguments: <config bucket name> <bulk import job ID> <bulk import task ID>");
        }
        String configBucket = args[0];
        String jobId = args[1];
        String taskId = args[2];

        InstanceProperties instanceProperties = new InstanceProperties();
        AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();

        try {
            instanceProperties.loadFromS3(amazonS3, configBucket);
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
        String jsonJobKey = "bulk_import/" + jobId + ".json";
        LOGGER.info("Loading bulk import job from key {} in bulk import bucket {}", bulkImportBucket, jsonJobKey);
        String jsonJob = amazonS3.getObjectAsString(bulkImportBucket, jsonJobKey);
        BulkImportJob bulkImportJob;
        try {
            bulkImportJob = new BulkImportJobSerDe().fromJson(jsonJob);
        } catch (JsonSyntaxException e) {
            LOGGER.error("Json job was malformed: {}", jobId);
            throw e;
        }

        BulkImportJobDriver driver = new BulkImportJobDriver(partitioner, instanceProperties,
                amazonS3, AmazonDynamoDBClientBuilder.defaultClient());
        driver.run(bulkImportJob, taskId);
    }
}

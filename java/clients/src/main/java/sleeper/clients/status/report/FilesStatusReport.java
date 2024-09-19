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
package sleeper.clients.status.report;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import sleeper.clients.status.report.filestatus.CVSFileStatusReporter;
import sleeper.clients.status.report.filestatus.FileStatusCollector;
import sleeper.clients.status.report.filestatus.FileStatusReporter;
import sleeper.clients.status.report.filestatus.JsonFileStatusReporter;
import sleeper.clients.status.report.filestatus.StandardFileStatusReporter;
import sleeper.clients.status.report.filestatus.TableFilesStatus;
import sleeper.clients.util.ClientUtils;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.statestore.StateStoreFactory;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;
import static sleeper.io.parquet.utils.HadoopConfigurationProvider.getConfigurationForClient;

/**
 * A utility class to report information about the files in the system and their
 * status.
 */
public class FilesStatusReport {
    private final int maxNumberOfFilesWithNoReferencesToCount;
    private final boolean verbose;
    private final FileStatusReporter fileStatusReporter;
    private final FileStatusCollector fileStatusCollector;

    private static final String DEFAULT_STATUS_REPORTER = "STANDARD";
    private static final Map<String, FileStatusReporter> FILE_STATUS_REPORTERS = new HashMap<>();

    static {
        FILE_STATUS_REPORTERS.put(DEFAULT_STATUS_REPORTER, new StandardFileStatusReporter());
        FILE_STATUS_REPORTERS.put("JSON", new JsonFileStatusReporter());
        FILE_STATUS_REPORTERS.put("CSV", new CVSFileStatusReporter());
    }

    public FilesStatusReport(StateStore stateStore, int maxNumberOfFilesWithNoReferencesToCount, boolean verbose) {
        this(stateStore, maxNumberOfFilesWithNoReferencesToCount, verbose, DEFAULT_STATUS_REPORTER);
    }

    public FilesStatusReport(
            StateStore stateStore, int maxNumberOfFilesWithNoReferencesToCount, boolean verbose, String outputType) {
        this(stateStore, maxNumberOfFilesWithNoReferencesToCount, verbose, getReporter(outputType));
    }

    public FilesStatusReport(
            StateStore stateStore, int maxNumberOfFilesWithNoReferencesToCount, boolean verbose,
            FileStatusReporter fileStatusReporter) {
        this.maxNumberOfFilesWithNoReferencesToCount = maxNumberOfFilesWithNoReferencesToCount;
        this.verbose = verbose;
        this.fileStatusReporter = fileStatusReporter;
        this.fileStatusCollector = new FileStatusCollector(stateStore);
    }

    private static FileStatusReporter getReporter(String outputType) {
        if (!FILE_STATUS_REPORTERS.containsKey(outputType)) {
            throw new IllegalArgumentException("Output type not supported " + outputType);
        }
        return FILE_STATUS_REPORTERS.get(outputType);
    }

    public void run() throws StateStoreException {
        TableFilesStatus tableStatus = fileStatusCollector.run(this.maxNumberOfFilesWithNoReferencesToCount);
        fileStatusReporter.report(tableStatus, verbose);
    }

    public static void main(String[] args) throws StateStoreException {
        if (!(args.length >= 2 && args.length <= 5)) {
            throw new IllegalArgumentException(
                    "Usage: <instance-id> <table-name> <optional-max-num-files-with-no-references-to-count> " +
                            "<optional-verbose-true-or-false> <optional-report-type-standard-or-csv-or-json>");
        }

        boolean verbose = false;
        int maxFilesWithNoReferences = 1000;
        String instanceId = args[0];
        String tableName = args[1];
        String reporterType = DEFAULT_STATUS_REPORTER;

        if (args.length >= 3) {
            maxFilesWithNoReferences = Integer.parseInt(args[2]);
        }

        if (args.length >= 4) {
            verbose = Boolean.parseBoolean(args[3]);
        }

        if (args.length >= 5) {
            reporterType = args[4].toUpperCase(Locale.ROOT);
        }

        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamoDBClient = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());

        try {
            InstanceProperties instanceProperties = ClientUtils.getInstanceProperties(s3Client, instanceId);
            TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, s3Client, dynamoDBClient);
            StateStoreFactory stateStoreFactory = new StateStoreFactory(instanceProperties, s3Client, dynamoDBClient, getConfigurationForClient());
            StateStore stateStore = stateStoreFactory.getStateStore(tablePropertiesProvider.getByName(tableName));
            new FilesStatusReport(stateStore, maxFilesWithNoReferences, verbose, reporterType).run();
        } finally {
            s3Client.shutdown();
            dynamoDBClient.shutdown();
        }
    }
}

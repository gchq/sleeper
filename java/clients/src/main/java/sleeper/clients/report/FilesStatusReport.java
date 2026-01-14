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
package sleeper.clients.report;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.clients.report.filestatus.CVSFileStatusReporter;
import sleeper.clients.report.filestatus.FileStatusCollector;
import sleeper.clients.report.filestatus.FileStatusReporter;
import sleeper.clients.report.filestatus.JsonFileStatusReporter;
import sleeper.clients.report.filestatus.StandardFileStatusReporter;
import sleeper.clients.report.filestatus.TableFilesStatus;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStore;
import sleeper.statestore.StateStoreFactory;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;

/**
 * Creates reports on the files in a Sleeper table.
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

    /**
     * Creates a report.
     */
    public void run() {
        TableFilesStatus tableStatus = fileStatusCollector.run(maxNumberOfFilesWithNoReferencesToCount);
        fileStatusReporter.report(tableStatus, verbose);
    }

    public static void main(String[] args) {
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

        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder())) {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
            TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient);
            StateStoreFactory stateStoreFactory = new StateStoreFactory(instanceProperties, s3Client, dynamoClient);
            StateStore stateStore = stateStoreFactory.getStateStore(tablePropertiesProvider.getByName(tableName));
            new FilesStatusReport(stateStore, maxFilesWithNoReferences, verbose, reporterType).run();
        }
    }
}

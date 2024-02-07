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

package sleeper.systemtest.suite.dsl;

import software.amazon.awscdk.NestedStack;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.InstanceProperty;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.systemtest.datageneration.RecordNumbers;
import sleeper.systemtest.drivers.compaction.AwsCompactionReportsDriver;
import sleeper.systemtest.drivers.ingest.AwsIngestReportsDriver;
import sleeper.systemtest.drivers.ingest.PurgeQueueDriver;
import sleeper.systemtest.drivers.instance.AwsSleeperInstanceDriver;
import sleeper.systemtest.drivers.instance.AwsSleeperInstanceTablesDriver;
import sleeper.systemtest.drivers.instance.AwsSystemTestDeploymentDriver;
import sleeper.systemtest.drivers.instance.AwsSystemTestParameters;
import sleeper.systemtest.drivers.partitioning.AwsPartitionReportDriver;
import sleeper.systemtest.drivers.sourcedata.GeneratedIngestSourceFilesDriver;
import sleeper.systemtest.drivers.sourcedata.IngestSourceFilesContext;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.instance.SleeperInstanceContext;
import sleeper.systemtest.dsl.instance.SystemTestDeploymentContext;
import sleeper.systemtest.dsl.instance.SystemTestOptionalStacks;
import sleeper.systemtest.dsl.instance.SystemTestParameters;
import sleeper.systemtest.dsl.instance.SystemTestTableFiles;
import sleeper.systemtest.dsl.instance.SystemTestTables;
import sleeper.systemtest.dsl.reporting.ReportingContext;
import sleeper.systemtest.dsl.reporting.SystemTestReporting;
import sleeper.systemtest.dsl.reporting.SystemTestReports;
import sleeper.systemtest.dsl.sourcedata.GenerateNumberedValueOverrides;
import sleeper.systemtest.dsl.sourcedata.SystemTestLocalFiles;
import sleeper.systemtest.suite.dsl.ingest.SystemTestIngest;
import sleeper.systemtest.suite.dsl.python.SystemTestPythonApi;
import sleeper.systemtest.suite.dsl.query.SystemTestQuery;
import sleeper.systemtest.suite.dsl.sourcedata.SystemTestCluster;
import sleeper.systemtest.suite.dsl.sourcedata.SystemTestSourceFiles;
import sleeper.systemtest.suite.fixtures.SystemTestInstance;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

/**
 * This class is the entry point that all system tests use to interact with the system.
 * It's the starting point for all steps of the Domain Specific Language (DSL) for system tests.
 * The purpose of this is to make it as easy as possible to read and write system tests.
 * It should make it easy to find and reuse steps to interact with the systems you care about for your tests.
 * <p>
 * It does this by delegating to drivers to actually interact with the system. The DSL only defines
 * the steps and language to be used by tests, and not any of the implemented behaviour of the steps.
 * <p>
 * This class should match as closely as possible with a diagram of the components of the system. You can
 * expect to find the system you care about through a method on this class. Some core features of the system are
 * on this class directly, but we try to limit its size by grouping methods into components which you access through
 * a method, eg. {@link #ingest()}.
 * <p>
 * Most tests will use steps from different systems, but where multiple steps run against the same system in succession
 * we can use method chaining to avoid accessing that system repeatedly. If you go from one system to another and back,
 * assume you should re-access the first system from here again the second time.
 * Try to avoid assigning variables except for data you want to reuse.
 */
public class SleeperSystemTest {
    private static final SleeperSystemTest INSTANCE = new SleeperSystemTest();

    private final SystemTestParameters parameters = AwsSystemTestParameters.loadFromSystemProperties();
    private final SystemTestClients clients = new SystemTestClients();
    private final SystemTestDeploymentContext systemTest = new SystemTestDeploymentContext(
            parameters, new AwsSystemTestDeploymentDriver(parameters, clients));
    private final SleeperInstanceContext instance = new SleeperInstanceContext(parameters, systemTest,
            new AwsSleeperInstanceDriver(parameters, clients), new AwsSleeperInstanceTablesDriver(clients));
    private final IngestSourceFilesContext sourceFiles = new IngestSourceFilesContext(systemTest, instance);
    private final ReportingContext reportingContext = new ReportingContext(parameters);
    private final PurgeQueueDriver purgeQueueDriver = new PurgeQueueDriver(instance, clients.getSqs());

    private SleeperSystemTest() {
    }

    public static SleeperSystemTest getInstance() {
        return INSTANCE.reset();
    }

    private SleeperSystemTest reset() {
        try {
            systemTest.deployIfMissing();
            systemTest.resetProperties();
            sourceFiles.reset();
            new GeneratedIngestSourceFilesDriver(systemTest, clients.getS3V2())
                    .emptyBucket();
            instance.disconnect();
            reportingContext.startRecording();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    public void connectToInstance(SystemTestInstance testInstance) {
        instance.connectTo(testInstance.getConfiguration());
        instance.resetPropertiesAndTables();
    }

    public void connectToInstanceNoTables(SystemTestInstance testInstance) {
        instance.connectTo(testInstance.getConfiguration());
        instance.resetPropertiesAndDeleteTables();
    }

    public InstanceProperties instanceProperties() {
        return instance.getInstanceProperties();
    }

    public TableProperties tableProperties() {
        return instance.getTableProperties();
    }

    public void updateTableProperties(Map<TableProperty, String> values) {
        instance.updateTableProperties(values);
    }

    public SystemTestSourceFiles sourceFiles() {
        return new SystemTestSourceFiles(instance, sourceFiles);
    }

    public SystemTestTableFiles tableFiles() {
        return new SystemTestTableFiles(instance);
    }

    public SystemTestPartitioning partitioning() {
        return new SystemTestPartitioning(instance, clients);
    }

    public SystemTestIngest ingest() {
        return new SystemTestIngest(clients, instance, sourceFiles);
    }

    public void purgeQueues(List<InstanceProperty> properties) throws InterruptedException {
        purgeQueueDriver.purgeQueues(properties);
    }

    public SystemTestQuery query() {
        return new SystemTestQuery(instance, clients);
    }

    public SystemTestQuery directQuery() {
        return query().direct();
    }

    public SystemTestCompaction compaction() {
        return new SystemTestCompaction(instance, clients);
    }

    public SystemTestReporting reporting() {
        return new SystemTestReporting(reportingContext,
                new AwsIngestReportsDriver(instance, clients),
                new AwsCompactionReportsDriver(instance, clients.getDynamoDB()));
    }

    public SystemTestReports.SystemTestBuilder reportsForExtension() {
        return SystemTestReports.builder(reportingContext,
                new AwsPartitionReportDriver(instance),
                new AwsIngestReportsDriver(instance, clients),
                new AwsCompactionReportsDriver(instance, clients.getDynamoDB()));
    }

    public SystemTestCluster systemTestCluster() {
        return new SystemTestCluster(clients, systemTest, instance);
    }

    public SystemTestPythonApi pythonApi() {
        return new SystemTestPythonApi(instance, clients, parameters.getPythonDirectory());
    }

    public SystemTestLocalFiles localFiles(Path tempDir) {
        return new SystemTestLocalFiles(instance, tempDir);
    }

    public void setGeneratorOverrides(GenerateNumberedValueOverrides overrides) {
        instance.setGeneratorOverrides(overrides);
    }

    public Iterable<Record> generateNumberedRecords(LongStream numbers) {
        return () -> instance.generateNumberedRecords(numbers).iterator();
    }

    public Iterable<Record> generateNumberedRecords(Schema schema, LongStream numbers) {
        return () -> instance.generateNumberedRecords(schema, numbers).iterator();
    }

    public RecordNumbers scrambleNumberedRecords(LongStream longStream) {
        return RecordNumbers.scrambleNumberedRecords(longStream);
    }

    public Path getSplitPointsDirectory() {
        return parameters.getScriptsDirectory()
                .resolve("test/splitpoints");
    }

    public <T extends NestedStack> void enableOptionalStack(Class<T> stackClass) {
        new SystemTestOptionalStacks(instance).addOptionalStack(stackClass);
    }

    public <T extends NestedStack> void disableOptionalStack(Class<T> stackClass) {
        new SystemTestOptionalStacks(instance).removeOptionalStack(stackClass);
    }

    public SystemTestTables tables() {
        return new SystemTestTables(instance);
    }
}

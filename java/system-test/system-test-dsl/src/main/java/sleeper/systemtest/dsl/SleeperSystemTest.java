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

package sleeper.systemtest.dsl;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.systemtest.dsl.compaction.SystemTestCompaction;
import sleeper.systemtest.dsl.ingest.SystemTestIngest;
import sleeper.systemtest.dsl.instance.SleeperInstanceContext;
import sleeper.systemtest.dsl.instance.SystemTestDeploymentContext;
import sleeper.systemtest.dsl.instance.SystemTestInstanceConfiguration;
import sleeper.systemtest.dsl.instance.SystemTestOptionalStacks;
import sleeper.systemtest.dsl.instance.SystemTestParameters;
import sleeper.systemtest.dsl.instance.SystemTestTableFiles;
import sleeper.systemtest.dsl.instance.SystemTestTables;
import sleeper.systemtest.dsl.partitioning.SystemTestPartitioning;
import sleeper.systemtest.dsl.python.SystemTestPythonApi;
import sleeper.systemtest.dsl.query.SystemTestQuery;
import sleeper.systemtest.dsl.reporting.ReportingContext;
import sleeper.systemtest.dsl.reporting.SystemTestReporting;
import sleeper.systemtest.dsl.sourcedata.GenerateNumberedValueOverrides;
import sleeper.systemtest.dsl.sourcedata.IngestSourceFilesContext;
import sleeper.systemtest.dsl.sourcedata.RecordNumbers;
import sleeper.systemtest.dsl.sourcedata.SystemTestCluster;
import sleeper.systemtest.dsl.sourcedata.SystemTestLocalFiles;
import sleeper.systemtest.dsl.sourcedata.SystemTestSourceFiles;
import sleeper.systemtest.dsl.util.SystemTestDrivers;

import java.nio.file.Path;
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

    private final SystemTestParameters parameters;
    private final SystemTestDrivers drivers;
    private final SystemTestDeploymentContext systemTest;
    private final SleeperInstanceContext instance;
    private final IngestSourceFilesContext sourceFiles;
    private final ReportingContext reportingContext;

    public SleeperSystemTest(SystemTestParameters parameters, SystemTestDrivers drivers) {
        this.parameters = parameters;
        this.drivers = drivers;
        systemTest = drivers.getSystemTestContext();
        instance = drivers.getInstanceContext();
        sourceFiles = drivers.getSourceFilesContext();
        reportingContext = drivers.getReportingContext();
    }

    public void reset() {
        try {
            systemTest.deployIfMissing();
            systemTest.resetProperties();
            sourceFiles.reset();
            drivers.generatedSourceFilesDriver().emptyBucket();
            instance.disconnect();
            reportingContext.startRecording();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void connectToInstance(SystemTestInstanceConfiguration configuration) {
        instance.connectTo(configuration);
        instance.resetPropertiesAndTables();
    }

    public void connectToInstanceNoTables(SystemTestInstanceConfiguration configuration) {
        instance.connectTo(configuration);
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
        return drivers.sourceFiles();
    }

    public SystemTestTableFiles tableFiles() {
        return new SystemTestTableFiles(instance);
    }

    public SystemTestPartitioning partitioning() {
        return drivers.partitioning();
    }

    public SystemTestIngest ingest() {
        return drivers.ingest();
    }

    public SystemTestQuery query() {
        return drivers.query();
    }

    public SystemTestQuery directQuery() {
        return query().direct();
    }

    public SystemTestCompaction compaction() {
        return drivers.compaction();
    }

    public SystemTestReporting reporting() {
        return drivers.reporting();
    }

    public SystemTestCluster systemTestCluster() {
        return drivers.systemTestCluster();
    }

    public SystemTestPythonApi pythonApi() {
        return drivers.pythonApi();
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

    public <T> void enableOptionalStack(Class<T> stackClass) {
        new SystemTestOptionalStacks(instance).addOptionalStack(stackClass);
    }

    public <T> void disableOptionalStack(Class<T> stackClass) {
        new SystemTestOptionalStacks(instance).removeOptionalStack(stackClass);
    }

    public SystemTestTables tables() {
        return new SystemTestTables(instance);
    }
}

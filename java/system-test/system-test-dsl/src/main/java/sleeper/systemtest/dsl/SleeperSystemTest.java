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

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TableProperty;
import sleeper.core.properties.validation.OptionalStack;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.systemtest.dsl.compaction.SystemTestCompaction;
import sleeper.systemtest.dsl.gc.SystemTestGarbageCollection;
import sleeper.systemtest.dsl.ingest.SystemTestIngest;
import sleeper.systemtest.dsl.instance.SystemTestInstanceConfiguration;
import sleeper.systemtest.dsl.instance.SystemTestOptionalStacks;
import sleeper.systemtest.dsl.instance.SystemTestParameters;
import sleeper.systemtest.dsl.instance.SystemTestTableFiles;
import sleeper.systemtest.dsl.instance.SystemTestTables;
import sleeper.systemtest.dsl.metrics.SystemTestTableMetrics;
import sleeper.systemtest.dsl.partitioning.SystemTestPartitioning;
import sleeper.systemtest.dsl.python.SystemTestPythonApi;
import sleeper.systemtest.dsl.query.SystemTestQuery;
import sleeper.systemtest.dsl.reporting.SystemTestReporting;
import sleeper.systemtest.dsl.sourcedata.GenerateNumberedValueOverrides;
import sleeper.systemtest.dsl.sourcedata.RecordNumbers;
import sleeper.systemtest.dsl.sourcedata.SystemTestCluster;
import sleeper.systemtest.dsl.sourcedata.SystemTestLocalFiles;
import sleeper.systemtest.dsl.sourcedata.SystemTestSourceFiles;
import sleeper.systemtest.dsl.statestore.SystemTestStateStore;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.stream.LongStream;

/**
 * The entry point that all system tests use to interact with the system. This is the starting point for all steps of
 * the Domain Specific Language (DSL) for system tests. The purpose of this is to make it as easy as possible to read
 * and write system tests. It should make it easy to find and reuse steps to interact with the systems you care about
 * for your tests.
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
    private final SystemTestDrivers baseDrivers;
    private final SystemTestContext context;

    public SleeperSystemTest(SystemTestParameters parameters, SystemTestDrivers baseDrivers, SystemTestContext context) {
        this.parameters = parameters;
        this.baseDrivers = baseDrivers;
        this.context = context;
    }

    public void connectToInstance(SystemTestInstanceConfiguration configuration) {
        context.instance().connectTo(configuration);
        context.instance().addDefaultTables();
    }

    public void connectToInstanceNoTables(SystemTestInstanceConfiguration configuration) {
        context.instance().connectTo(configuration);
    }

    public InstanceProperties instanceProperties() {
        return context.instance().getInstanceProperties();
    }

    public TableProperties tableProperties() {
        return context.instance().getTableProperties();
    }

    public void updateTableProperties(Map<TableProperty, String> values) {
        context.instance().updateTableProperties(values);
    }

    public SystemTestSourceFiles sourceFiles() {
        return new SystemTestSourceFiles(context.instance(), context.sourceFiles(), baseDrivers.sourceFiles(context));
    }

    public SystemTestTableFiles tableFiles() {
        return new SystemTestTableFiles(context.instance());
    }

    public SystemTestPartitioning partitioning() {
        return new SystemTestPartitioning(context);
    }

    public SystemTestIngest ingest() {
        return new SystemTestIngest(context, baseDrivers);
    }

    public SystemTestQuery query() {
        return new SystemTestQuery(context, baseDrivers);
    }

    public SystemTestQuery directQuery() {
        return query().direct();
    }

    public SystemTestCompaction compaction() {
        return new SystemTestCompaction(context);
    }

    public SystemTestGarbageCollection garbageCollection() {
        return new SystemTestGarbageCollection(context);
    }

    public SystemTestReporting reporting() {
        return new SystemTestReporting(context);
    }

    public SystemTestTableMetrics tableMetrics() {
        return new SystemTestTableMetrics(context.instance().adminDrivers().tableMetrics(context));
    }

    public SystemTestCluster systemTestCluster() {
        return new SystemTestCluster(context, baseDrivers);
    }

    public SystemTestPythonApi pythonApi() {
        return new SystemTestPythonApi(context);
    }

    public SystemTestLocalFiles localFiles(Path tempDir) {
        return new SystemTestLocalFiles(context.instance(), baseDrivers.localFiles(context), tempDir);
    }

    public void setGeneratorOverrides(GenerateNumberedValueOverrides overrides) {
        context.instance().setGeneratorOverrides(overrides);
    }

    public Iterable<Record> generateNumberedRecords(LongStream numbers) {
        return () -> context.instance().generateNumberedRecords(numbers).iterator();
    }

    public Iterable<Record> generateNumberedRecords(Schema schema, LongStream numbers) {
        return () -> context.instance().generateNumberedRecords(schema, numbers).iterator();
    }

    public RecordNumbers scrambleNumberedRecords(LongStream longStream) {
        return RecordNumbers.scrambleNumberedRecords(longStream);
    }

    public Path getSplitPointsDirectory() {
        return parameters.getScriptsDirectory()
                .resolve("test/splitpoints");
    }

    public void enableOptionalStack(OptionalStack stack) {
        new SystemTestOptionalStacks(context.instance()).addOptionalStack(stack);
    }

    public void enableOptionalStacks(Collection<OptionalStack> stacks) {
        new SystemTestOptionalStacks(context.instance()).addOptionalStacks(stacks);
    }

    public void disableOptionalStack(OptionalStack stack) {
        new SystemTestOptionalStacks(context.instance()).removeOptionalStack(stack);
    }

    public void disableOptionalStacks(Collection<OptionalStack> stacks) {
        new SystemTestOptionalStacks(context.instance()).removeOptionalStacks(stacks);
    }

    public SystemTestStateStore stateStore() {
        return new SystemTestStateStore(context);
    }

    public SystemTestTables tables() {
        return new SystemTestTables(context.instance());
    }

    public SleeperSystemTest table(String name) {
        context.instance().setCurrentTable(name);
        return this;
    }
}

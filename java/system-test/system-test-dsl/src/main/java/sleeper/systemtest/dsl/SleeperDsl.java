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

package sleeper.systemtest.dsl;

import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.OptionalStack;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TableProperty;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.systemtest.dsl.bulkexport.BulkExportDsl;
import sleeper.systemtest.dsl.compaction.CompactionDsl;
import sleeper.systemtest.dsl.gc.SystemTestGarbageCollection;
import sleeper.systemtest.dsl.ingest.IngestDsl;
import sleeper.systemtest.dsl.instance.DataFilesDriver;
import sleeper.systemtest.dsl.instance.SystemTestInstanceConfiguration;
import sleeper.systemtest.dsl.instance.SystemTestOptionalStacks;
import sleeper.systemtest.dsl.instance.SystemTestParameters;
import sleeper.systemtest.dsl.instance.SystemTestTableFiles;
import sleeper.systemtest.dsl.instance.SystemTestTables;
import sleeper.systemtest.dsl.metrics.TableMetricsDsl;
import sleeper.systemtest.dsl.partitioning.PartitioningDsl;
import sleeper.systemtest.dsl.python.PythonApiDsl;
import sleeper.systemtest.dsl.query.QueryDsl;
import sleeper.systemtest.dsl.reporting.ReportingDsl;
import sleeper.systemtest.dsl.sourcedata.DataGenerationClusterDsl;
import sleeper.systemtest.dsl.sourcedata.GenerateNumberedRows;
import sleeper.systemtest.dsl.sourcedata.GenerateNumberedValueOverrides;
import sleeper.systemtest.dsl.sourcedata.LocalFilesDsl;
import sleeper.systemtest.dsl.sourcedata.RowNumbers;
import sleeper.systemtest.dsl.sourcedata.SourceFilesDsl;
import sleeper.systemtest.dsl.statestore.StateStoreDSl;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.stream.LongStream;

import static sleeper.core.properties.table.TableProperty.TABLE_ONLINE;

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
public class SleeperDsl {

    private final SystemTestParameters parameters;
    private final SystemTestDrivers baseDrivers;
    private final SystemTestContext context;

    public SleeperDsl(SystemTestParameters parameters, SystemTestDrivers baseDrivers, SystemTestContext context) {
        this.parameters = parameters;
        this.baseDrivers = baseDrivers;
        this.context = context;
    }

    public void connectToInstanceAddOnlineTable(SystemTestInstanceConfiguration configuration) {
        context.instance().connectTo(configuration);
        context.instance().addDefaultTables(Map.of(TABLE_ONLINE, "true"));
    }

    public void connectToInstanceAddOfflineTable(SystemTestInstanceConfiguration configuration) {
        context.instance().connectTo(configuration);
        context.instance().addDefaultTables(Map.of(TABLE_ONLINE, "false"));
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

    public SourceFilesDsl sourceFiles() {
        return new SourceFilesDsl(context, baseDrivers.sourceFiles(context));
    }

    public SystemTestTableFiles tableFiles() {
        return new SystemTestTableFiles(context.instance());
    }

    public PartitioningDsl partitioning() {
        return new PartitioningDsl(context);
    }

    public IngestDsl ingest() {
        return new IngestDsl(context, baseDrivers);
    }

    public QueryDsl query() {
        return new QueryDsl(context, baseDrivers);
    }

    public QueryDsl directQuery() {
        return query().direct();
    }

    public BulkExportDsl bulkExport() {
        return new BulkExportDsl(context);
    }

    public CompactionDsl compaction() {
        return new CompactionDsl(context, baseDrivers);
    }

    public SystemTestGarbageCollection garbageCollection() {
        return new SystemTestGarbageCollection(context);
    }

    public ReportingDsl reporting() {
        return new ReportingDsl(context);
    }

    public TableMetricsDsl tableMetrics() {
        return new TableMetricsDsl(context.instance().adminDrivers().tableMetrics(context));
    }

    public DataGenerationClusterDsl systemTestCluster() {
        return new DataGenerationClusterDsl(context, baseDrivers);
    }

    public PythonApiDsl pythonApi() {
        return new PythonApiDsl(context);
    }

    public LocalFilesDsl localFiles(Path tempDir) {
        return new LocalFilesDsl(context.instance(), baseDrivers.localFiles(context), tempDir);
    }

    public void setGeneratorOverrides(GenerateNumberedValueOverrides overrides) {
        context.instance().setGeneratorOverrides(overrides);
    }

    public GenerateNumberedRows numberedRows() {
        return context.instance().numberedRows();
    }

    public GenerateNumberedRows generateNumberedRows() {
        return context.instance().numberedRows();
    }

    public GenerateNumberedRows generateNumberedRows(Schema schema) {
        return context.instance().numberedRows(schema);
    }

    public RowNumbers scrambleNumberedRows(LongStream longStream) {
        return RowNumbers.scrambleNumberedRows(longStream);
    }

    public Path getSplitPointsDirectory() {
        return parameters.getScriptsDirectory()
                .resolve("test/splitpoints");
    }

    public CloseableIterator<Row> getRows(AllReferencesToAFile file) {
        Schema schema = context.instance().getTableProperties().getSchema();
        DataFilesDriver driver = context.instance().adminDrivers().dataFiles(context);
        return driver.getRows(schema, file.getFilename());
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

    public StateStoreDSl stateStore() {
        return new StateStoreDSl(context);
    }

    public SystemTestTables tables() {
        return new SystemTestTables(context.instance());
    }

    public SleeperDsl table(String name) {
        context.instance().setCurrentTable(name);
        return this;
    }
}

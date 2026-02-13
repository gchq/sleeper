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
package sleeper.cdk.stack;

import software.constructs.Construct;

import sleeper.cdk.SleeperInstanceProps;
import sleeper.cdk.artefacts.SleeperArtefacts;
import sleeper.cdk.artefacts.SleeperJarVersionIdsCache;
import sleeper.cdk.stack.bulkexport.BulkExportStack;
import sleeper.cdk.stack.bulkimport.BulkImportBucketStack;
import sleeper.cdk.stack.bulkimport.CommonEmrBulkImportStack;
import sleeper.cdk.stack.bulkimport.EksBulkImportStack;
import sleeper.cdk.stack.bulkimport.EmrBulkImportStack;
import sleeper.cdk.stack.bulkimport.EmrServerlessBulkImportStack;
import sleeper.cdk.stack.bulkimport.EmrStudioStack;
import sleeper.cdk.stack.bulkimport.PersistentEmrBulkImportStack;
import sleeper.cdk.stack.compaction.CompactionStack;
import sleeper.cdk.stack.core.AutoStopEmrServerlessApplicationStack;
import sleeper.cdk.stack.ingest.IngestBatcherStack;
import sleeper.cdk.stack.ingest.IngestStack;
import sleeper.cdk.stack.ingest.IngestStacks;
import sleeper.cdk.stack.query.KeepLambdaWarmStack;
import sleeper.cdk.stack.query.QueryQueueStack;
import sleeper.cdk.stack.query.QueryStack;
import sleeper.cdk.stack.query.WebSocketQueryStack;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.OptionalStack;

import java.util.Set;

import static java.util.stream.Collectors.toUnmodifiableSet;
import static sleeper.core.properties.instance.CommonProperty.OPTIONAL_STACKS;

public class SleeperOptionalStacks {

    private SleeperOptionalStacks() {
    }

    public static void create(
            Construct scope, SleeperInstanceProps props, SleeperCoreStacks coreStacks) {
        InstanceProperties instanceProperties = props.getInstanceProperties();
        SleeperJarVersionIdsCache jars = props.getJars();
        SleeperArtefacts artefacts = props.getArtefacts();
        Set<OptionalStack> optionalStacks = instanceProperties
                .streamEnumList(OPTIONAL_STACKS, OptionalStack.class)
                .collect(toUnmodifiableSet());
        if (optionalStacks.contains(OptionalStack.TableMetricsStack)) {
            new TableMetricsStack(scope, "TableMetrics", props, coreStacks);
        }

        if (optionalStacks.contains(OptionalStack.AthenaStack)) {
            new AthenaStack(scope, "Athena", instanceProperties, artefacts, coreStacks);
        }

        BulkImportBucketStack bulkImportBucketStack = null;
        if (OptionalStack.BULK_IMPORT_STACKS.stream().anyMatch(optionalStacks::contains)) {
            bulkImportBucketStack = new BulkImportBucketStack(scope, "BulkImportBucket", instanceProperties, coreStacks, jars);
        }
        CommonEmrBulkImportStack emrBulkImportCommonStack = null;
        if (OptionalStack.EMR_BULK_IMPORT_STACKS.stream().anyMatch(optionalStacks::contains)) {
            emrBulkImportCommonStack = new CommonEmrBulkImportStack(scope, "BulkImportEMRCommon",
                    instanceProperties, coreStacks, bulkImportBucketStack);
        }

        // Stack to run bulk import jobs via EMR Serverless
        EmrServerlessBulkImportStack emrServerlessBulkImportStack = null;
        if (optionalStacks.contains(OptionalStack.EmrServerlessBulkImportStack)) {
            AutoStopEmrServerlessApplicationStack autoStopEmrServerlessStack = new AutoStopEmrServerlessApplicationStack(
                    scope, "AutoStopEmrServerlessApplication", instanceProperties, jars, coreStacks.getLoggingStack());
            emrServerlessBulkImportStack = new EmrServerlessBulkImportStack(scope, "BulkImportEMRServerless",
                    instanceProperties, artefacts,
                    bulkImportBucketStack,
                    coreStacks,
                    autoStopEmrServerlessStack);

            // Stack to created EMR studio to be used to access EMR Serverless
            if (optionalStacks.contains(OptionalStack.EmrStudioStack)) {
                new EmrStudioStack(scope, "EmrStudio", instanceProperties, coreStacks);
            }
        }
        // Stack to run bulk import jobs via EMR (one cluster per bulk import job)
        EmrBulkImportStack emrBulkImportStack = null;
        if (optionalStacks.contains(OptionalStack.EmrBulkImportStack)) {
            emrBulkImportStack = new EmrBulkImportStack(scope, "BulkImportEMR",
                    instanceProperties, artefacts,
                    bulkImportBucketStack,
                    emrBulkImportCommonStack,
                    coreStacks);
        }

        // Stack to run bulk import jobs via a persistent EMR cluster
        PersistentEmrBulkImportStack persistentEmrBulkImportStack = null;
        if (optionalStacks.contains(OptionalStack.PersistentEmrBulkImportStack)) {
            persistentEmrBulkImportStack = new PersistentEmrBulkImportStack(scope, "BulkImportPersistentEMR",
                    instanceProperties, artefacts,
                    bulkImportBucketStack,
                    emrBulkImportCommonStack,
                    coreStacks);
        }

        // Stack to run bulk import jobs via EKS
        EksBulkImportStack eksBulkImportStack = null;
        if (optionalStacks.contains(OptionalStack.EksBulkImportStack)) {
            eksBulkImportStack = new EksBulkImportStack(scope, "BulkImportEKS",
                    instanceProperties, jars,
                    bulkImportBucketStack,
                    coreStacks);
        }

        // Stack to run bulk export jobs
        if (optionalStacks.contains(OptionalStack.BulkExportStack)) {
            new BulkExportStack(scope, "BulkExport", props, coreStacks);
        }

        // Stack to garbage collect old files
        if (optionalStacks.contains(OptionalStack.GarbageCollectorStack)) {
            new GarbageCollectorStack(scope, "GarbageCollector", props, coreStacks);
        }
        // Stack for containers for compactions and splitting compactions
        CompactionStack compactionStack = null;
        if (optionalStacks.contains(OptionalStack.CompactionStack)) {
            compactionStack = new CompactionStack(scope, "Compaction", props, coreStacks);
        }

        // Stack to split partitions
        PartitionSplittingStack partitionSplittingStack = null;
        if (optionalStacks.contains(OptionalStack.PartitionSplittingStack)) {
            partitionSplittingStack = new PartitionSplittingStack(scope, "PartitionSplitting", props, coreStacks);
        }

        // Stack to execute queries
        QueryStack queryStack = null;
        QueryQueueStack queryQueueStack = null;
        if (OptionalStack.QUERY_STACKS.stream().anyMatch(optionalStacks::contains)) {
            queryQueueStack = new QueryQueueStack(scope, "QueryQueue",
                    instanceProperties, coreStacks);
            queryStack = new QueryStack(scope, "Query",
                    instanceProperties, jars, coreStacks,
                    queryQueueStack);
            // Stack to execute queries using the web socket API
            if (optionalStacks.contains(OptionalStack.WebSocketQueryStack)) {
                new WebSocketQueryStack(scope,
                        "WebSocketQuery",
                        instanceProperties, jars,
                        coreStacks, queryQueueStack, queryStack);
            }
        }
        // Stack for ingest jobs
        IngestStack ingestStack = null;
        if (optionalStacks.contains(OptionalStack.IngestStack)) {
            ingestStack = new IngestStack(scope, "Ingest", props, coreStacks);
        }

        // Aggregate ingest stacks
        IngestStacks ingestStacks = new IngestStacks(ingestStack, emrBulkImportStack, persistentEmrBulkImportStack, eksBulkImportStack, emrServerlessBulkImportStack);

        // Stack to batch up files to ingest and create jobs
        if (optionalStacks.contains(OptionalStack.IngestBatcherStack)) {
            new IngestBatcherStack(scope, "IngestBatcher", props, coreStacks, ingestStacks);
        }

        if (optionalStacks.contains(OptionalStack.DashboardStack)) {
            new DashboardStack(scope, "Dashboard",
                    ingestStack,
                    compactionStack,
                    partitionSplittingStack,
                    instanceProperties, props.getTableProperties(),
                    coreStacks.getErrorMetrics());
        }

        if (optionalStacks.contains(OptionalStack.KeepLambdaWarmStack)) {
            new KeepLambdaWarmStack(scope, "KeepLambdaWarmExecution", props, coreStacks, queryQueueStack);
        }
    }

}

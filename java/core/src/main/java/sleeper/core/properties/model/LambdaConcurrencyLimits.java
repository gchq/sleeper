/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.core.properties.model;

import sleeper.core.properties.SleeperProperties;
import sleeper.core.properties.SleeperPropertiesValidationCriteria;
import sleeper.core.properties.instance.InstanceProperty;

import java.util.List;

import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_COMMIT_BATCHER_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_COMMIT_BATCHER_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_CREATION_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_CREATION_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_DISPATCH_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_DISPATCH_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.instance.GarbageCollectionProperty.GARBAGE_COLLECTOR_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.GarbageCollectionProperty.GARBAGE_COLLECTOR_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.instance.MetricsProperty.METRICS_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.MetricsProperty.METRICS_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.instance.PartitionSplittingProperty.FIND_PARTITIONS_TO_SPLIT_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.PartitionSplittingProperty.FIND_PARTITIONS_TO_SPLIT_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.instance.TableStateProperty.SNAPSHOT_CREATION_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.TableStateProperty.SNAPSHOT_CREATION_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.instance.TableStateProperty.SNAPSHOT_DELETION_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.TableStateProperty.SNAPSHOT_DELETION_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.instance.TableStateProperty.STATESTORE_COMMITTER_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.TableStateProperty.STATESTORE_COMMITTER_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.instance.TableStateProperty.TRANSACTION_DELETION_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.TableStateProperty.TRANSACTION_DELETION_LAMBDA_CONCURRENCY_RESERVED;

/**
 * Compares reserved concurrency on a function with maximum concurrency on its SQS event source.
 *
 * @param reservedProperty the function's reserved concurrency property
 * @param maximumProperty  the SQS event source's maximum concurrency property
 */
public record LambdaConcurrencyLimits(InstanceProperty reservedProperty, InstanceProperty maximumProperty)
        implements SleeperPropertiesValidationCriteria<InstanceProperty> {

    /**
     * Creates validation criteria for the functions with configurable SQS maximum concurrency.
     * Each function has a single SQS event source.
     *
     * @return the validation criteria
     */
    public static List<SleeperPropertiesValidationCriteria<InstanceProperty>> validationCriteria() {
        return List.of(
                new LambdaConcurrencyLimits(METRICS_LAMBDA_CONCURRENCY_RESERVED, METRICS_LAMBDA_CONCURRENCY_MAXIMUM),
                new LambdaConcurrencyLimits(GARBAGE_COLLECTOR_LAMBDA_CONCURRENCY_RESERVED, GARBAGE_COLLECTOR_LAMBDA_CONCURRENCY_MAXIMUM),
                new LambdaConcurrencyLimits(FIND_PARTITIONS_TO_SPLIT_LAMBDA_CONCURRENCY_RESERVED, FIND_PARTITIONS_TO_SPLIT_LAMBDA_CONCURRENCY_MAXIMUM),
                new LambdaConcurrencyLimits(COMPACTION_JOB_CREATION_LAMBDA_CONCURRENCY_RESERVED, COMPACTION_JOB_CREATION_LAMBDA_CONCURRENCY_MAXIMUM),
                new LambdaConcurrencyLimits(COMPACTION_JOB_DISPATCH_LAMBDA_CONCURRENCY_RESERVED, COMPACTION_JOB_DISPATCH_LAMBDA_CONCURRENCY_MAXIMUM),
                new LambdaConcurrencyLimits(COMPACTION_COMMIT_BATCHER_LAMBDA_CONCURRENCY_RESERVED, COMPACTION_COMMIT_BATCHER_LAMBDA_CONCURRENCY_MAXIMUM),
                new LambdaConcurrencyLimits(STATESTORE_COMMITTER_LAMBDA_CONCURRENCY_RESERVED, STATESTORE_COMMITTER_LAMBDA_CONCURRENCY_MAXIMUM),
                new LambdaConcurrencyLimits(SNAPSHOT_CREATION_LAMBDA_CONCURRENCY_RESERVED, SNAPSHOT_CREATION_LAMBDA_CONCURRENCY_MAXIMUM),
                new LambdaConcurrencyLimits(SNAPSHOT_DELETION_LAMBDA_CONCURRENCY_RESERVED, SNAPSHOT_DELETION_LAMBDA_CONCURRENCY_MAXIMUM),
                new LambdaConcurrencyLimits(TRANSACTION_DELETION_LAMBDA_CONCURRENCY_RESERVED, TRANSACTION_DELETION_LAMBDA_CONCURRENCY_MAXIMUM));
    }

    @Override
    public List<InstanceProperty> getPropertiesValidated() {
        return List.of(reservedProperty, maximumProperty);
    }

    @Override
    public boolean test(SleeperProperties<InstanceProperty> properties) {
        Integer reserved = properties.getIntOrNull(reservedProperty);
        Integer maximum = properties.getIntOrNull(maximumProperty);
        return reserved == null || maximum == null || maximum <= reserved;
    }
}

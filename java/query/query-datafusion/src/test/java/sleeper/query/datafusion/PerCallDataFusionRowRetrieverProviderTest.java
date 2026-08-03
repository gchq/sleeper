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
package sleeper.query.datafusion;

import org.junit.jupiter.api.Test;

import sleeper.foreign.datafusion.DataFusionAwsConfig;
import sleeper.query.core.rowretrieval.LeafPartitionRowRetriever;

import static org.assertj.core.api.Assertions.assertThat;

public class PerCallDataFusionRowRetrieverProviderTest {
    @Test
    void shouldSupportFiltersAndAggregations() {
        // Given
        DataFusionAwsConfig awsConfig = DataFusionAwsConfig.overrideEndpoint("dummy");
        PerCallDataFusionRowRetrieverProvider provider = new PerCallDataFusionRowRetrieverProvider(awsConfig);

        // When
        LeafPartitionRowRetriever retriever = provider.getRowRetriever(null);

        // Then
        assertThat(retriever.supportsFiltersAndAggregations()).isTrue();
    }

    @Test
    void shouldSupportSqlFiltering() {
        // Given
        DataFusionAwsConfig awsConfig = DataFusionAwsConfig.overrideEndpoint("dummy");
        PerCallDataFusionRowRetrieverProvider provider = new PerCallDataFusionRowRetrieverProvider(awsConfig);

        // When
        LeafPartitionRowRetriever retriever = provider.getRowRetriever(null);

        // Then
        assertThat(retriever.supportsSqlFiltering()).isTrue();
    }
}

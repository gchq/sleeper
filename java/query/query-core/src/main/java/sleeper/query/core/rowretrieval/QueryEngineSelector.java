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
package sleeper.query.core.rowretrieval;

import sleeper.core.properties.model.DataEngine;
import sleeper.core.properties.table.TableProperties;

import static sleeper.core.properties.table.TableProperty.DATA_ENGINE;

/**
 * Selects a query engine based on the data engine chosen for a table.
 */
public class QueryEngineSelector implements LeafPartitionRowRetrieverProvider {

    private final LeafPartitionRowRetrieverProvider javaProvider;
    private final LeafPartitionRowRetrieverProvider dataFusionProvider;

    private QueryEngineSelector(LeafPartitionRowRetrieverProvider javaProvider, LeafPartitionRowRetrieverProvider dataFusionProvider) {
        this.javaProvider = javaProvider;
        this.dataFusionProvider = dataFusionProvider;
    }

    /**
     * Creates an engine selector from providers for the Java and DataFusion data engines.
     *
     * @param  javaProvider       the Java data engine provider
     * @param  dataFusionProvider the DataFusion data engine provider
     * @return                    the provider
     */
    public static LeafPartitionRowRetrieverProvider javaAndDataFusion(LeafPartitionRowRetrieverProvider javaProvider, LeafPartitionRowRetrieverProvider dataFusionProvider) {
        return new QueryEngineSelector(javaProvider, dataFusionProvider);
    }

    @Override
    public LeafPartitionRowRetriever getRowRetriever(TableProperties tableProperties) {
        DataEngine engine = tableProperties.getEnumValue(DATA_ENGINE, DataEngine.class);
        switch (engine) {
            case DATAFUSION_EXPERIMENTAL:
                return dataFusionProvider.getRowRetriever(tableProperties);
            case DATAFUSION:
            case JAVA:
            default:
                return javaProvider.getRowRetriever(tableProperties);
        }
    }

}

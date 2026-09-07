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
package sleeper.api.resources;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.OptionalStack;
import sleeper.core.table.TableStatus;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.REGION;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.OPTIONAL_STACKS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TRACKER_ENABLED;
import static sleeper.core.properties.instance.IngestProperty.INGEST_TRACKER_ENABLED;

@Path("/api/instance")
public class InstanceResource {

    private final S3Client s3Client;
    private final DynamoDbClient dynamoDbClient;
    private final String instanceId;
    private final String accountName;

    @Inject
    public InstanceResource(
        S3Client s3Client,
        DynamoDbClient dynamoDbClient,
        @ConfigProperty(name = "sleeper.instance.id") String instanceId,
        @ConfigProperty(name = "sleeper.account.name") String accountName
    ) {
        this.s3Client = s3Client;
        this.dynamoDbClient = dynamoDbClient;
        this.instanceId = instanceId;
        this.accountName = accountName;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public InstanceResponse getInstance() {
        InstanceProperties instanceProperties = S3InstanceProperties.loadGivenAccountAndInstanceId(s3Client, accountName, instanceId);
        DynamoDBTableIndex tableIndex = new DynamoDBTableIndex(instanceProperties, dynamoDbClient);
        List<TableStatus> tables = tableIndex.streamAllTables().collect(Collectors.toList());
        return new InstanceResponse(
                instanceProperties.get(ID),
                instanceProperties.get(VERSION),
                instanceProperties.get(REGION),
                tables,
                features(instanceProperties));
    }

    private static Map<String, Boolean> features(InstanceProperties instanceProperties) {
        Map<String, Boolean> features = new LinkedHashMap<>();

        Set<OptionalStack> deployedStacks = Set.copyOf(instanceProperties.getEnumList(OPTIONAL_STACKS, OptionalStack.class));
        for (OptionalStack stack : OptionalStack.all()) {
            features.put(stack.name(), deployedStacks.contains(stack));
        }

        features.put("IngestTracking", instanceProperties.getBoolean(INGEST_TRACKER_ENABLED));
        features.put("CompactionTracking", instanceProperties.getBoolean(COMPACTION_TRACKER_ENABLED));
        return features;
    }

    public record InstanceResponse(String instanceId, String version, String region, List<TableStatus> tables, Map<String, Boolean> features) {}

}

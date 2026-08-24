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

package sleeper.systemtest.drivers.instance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.ContentStreamProvider;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;
import software.amazon.awssdk.http.auth.spi.internal.signer.DefaultSignRequest;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.clients.table.AddTableClient;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.table.TableIndex;
import sleeper.restapi.addTable.AddTableRequest;
import sleeper.restapi.addTable.AddTableRequestSerDe;
import sleeper.statestore.StateStoreFactory;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.instance.SleeperTablesDriver;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.REGION;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.REST_API_URL;

public class AwsSleeperTablesDriver implements SleeperTablesDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(AwsSleeperTablesDriver.class);

    private final S3Client s3;
    private final DynamoDbClient dynamoDB;
    private final AwsCredentialsProvider credentialsProvider;
    private final HttpClient httpClient;

    public AwsSleeperTablesDriver(SystemTestClients clients) {
        this.s3 = clients.getS3();
        this.dynamoDB = clients.getDynamo();
        this.credentialsProvider = clients.getCredentialsProvider();
        this.httpClient = clients.getHttpClient();
    }

    @Override
    public void saveTableProperties(InstanceProperties instanceProperties, TableProperties tableProperties) {
        tablePropertiesStore(instanceProperties).save(tableProperties);
    }

    @Override
    public void addTable(InstanceProperties instanceProperties, TableProperties properties) {
        if (instanceProperties.isSet(REST_API_URL)) {
            LOGGER.info("Adding table via REST API");
            addTableViaRest(instanceProperties, properties);
        } else {
            LOGGER.info("Adding table directly");
            addTableDirectly(instanceProperties, properties);
        }
    }

    protected void addTableViaRest(InstanceProperties instanceProperties, TableProperties properties) {
        String body = new AddTableRequestSerDe(instanceProperties).toJson(AddTableRequest.builder()
                .properties(properties)
                .build());
        URI uri = addTableUri(instanceProperties);
        SignedRequest signedRequest = AwsV4HttpSigner.create().sign(DefaultSignRequest.builder(credentialsProvider.resolveCredentials())
                .putProperty(AwsV4HttpSigner.SERVICE_SIGNING_NAME, "execute-api")
                .putProperty(AwsV4HttpSigner.REGION_NAME, instanceProperties.get(REGION))
                .request(SdkHttpRequest.builder()
                        .uri(uri)
                        .protocol(uri.getScheme())
                        .method(SdkHttpMethod.POST)
                        .putHeader("Content-Type", "application/json")
                        .build())
                .payload(ContentStreamProvider.fromUtf8String(body))
                .build());
        try {
            HttpRequest.Builder request = HttpRequest.newBuilder(uri)
                    .POST(HttpRequest.BodyPublishers.ofString(body));
            signedRequest.request().forEachHeader((header, values) -> {
                if (!"Host".equalsIgnoreCase(header)) {
                    values.forEach(value -> request.header(header, value));
                }
            });
            var response = httpClient.send(request.build(), BodyHandlers.ofString());
            if (response.statusCode() != 201) {
                throw new RuntimeException("Failed to add table through REST API, status code: " + response.statusCode()
                        + ", response: " + response.body());
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    protected void addTableDirectly(InstanceProperties instanceProperties, TableProperties properties) {
        try {
            new AddTableClient(properties,
                    S3TableProperties.createStore(instanceProperties, s3, dynamoDB),
                    StateStoreFactory.createProvider(instanceProperties, s3, dynamoDB))
                    .run();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static URI addTableUri(InstanceProperties instanceProperties) {
        String restApiUrl = instanceProperties.get(REST_API_URL);
        return URI.create(restApiUrl + (restApiUrl.endsWith("/") ? "" : "/") + "sleeper/tables");
    }

    @Override
    public TablePropertiesProvider createTablePropertiesProvider(InstanceProperties instanceProperties) {
        return S3TableProperties.createProvider(instanceProperties, s3, dynamoDB);
    }

    @Override
    public StateStoreProvider createStateStoreProvider(InstanceProperties instanceProperties) {
        return StateStoreFactory.createProvider(instanceProperties, s3, dynamoDB);
    }

    @Override
    public TableIndex tableIndex(InstanceProperties instanceProperties) {
        return new DynamoDBTableIndex(instanceProperties, dynamoDB);
    }

    private TablePropertiesStore tablePropertiesStore(InstanceProperties instanceProperties) {
        return S3TableProperties.createStore(instanceProperties, s3, dynamoDB);
    }
}

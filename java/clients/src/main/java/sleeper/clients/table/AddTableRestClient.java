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

package sleeper.clients.table;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.ContentStreamProvider;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;
import software.amazon.awssdk.http.auth.spi.internal.signer.DefaultSignRequest;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.restapi.addTable.AddTableRequest;
import sleeper.restapi.addTable.AddTableRequestSerDe;
import sleeper.restapi.addTable.AddTableResponse;
import sleeper.restapi.addTable.AddTableResponseSerDe;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.REGION;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.REST_API_URL;

/**
 * Client for adding a table through the Sleeper REST API.
 */
public class AddTableRestClient implements AutoCloseable {
    private static final Duration CONNECTION_TIME_TO_LIVE = Duration.ofSeconds(30);

    private final InstanceProperties instanceProperties;
    private final AwsCredentialsProvider credentialsProvider;
    private final SdkHttpClient httpClient;

    public AddTableRestClient(InstanceProperties instanceProperties, AwsCredentialsProvider credentialsProvider) {
        this.instanceProperties = instanceProperties;
        this.credentialsProvider = credentialsProvider;
        this.httpClient = ApacheHttpClient.builder()
                .connectionTimeToLive(CONNECTION_TIME_TO_LIVE)
                .build();
    }

    /**
     * Adds a table through the REST API.
     *
     * @param  properties table properties, including the schema
     * @return            response containing the assigned table ID and name
     */
    public AddTableResponse addTable(TableProperties properties) {
        String body = new AddTableRequestSerDe(instanceProperties).toJson(AddTableRequest.builder()
                .properties(properties)
                .build());
        URI uri = addTableUri(instanceProperties);
        ContentStreamProvider payload = ContentStreamProvider.fromUtf8String(body);
        SignedRequest signedRequest = AwsV4HttpSigner.create().sign(DefaultSignRequest.builder(credentialsProvider.resolveCredentials())
                .putProperty(AwsV4HttpSigner.SERVICE_SIGNING_NAME, "execute-api")
                .putProperty(AwsV4HttpSigner.REGION_NAME, instanceProperties.get(REGION))
                .request(SdkHttpRequest.builder()
                        .uri(uri)
                        .protocol(uri.getScheme())
                        .method(SdkHttpMethod.POST)
                        .putHeader("Content-Type", "application/json")
                        .build())
                .payload(payload)
                .build());
        try {
            HttpExecuteResponse response = httpClient.prepareRequest(HttpExecuteRequest.builder()
                    .request(signedRequest.request())
                    .contentStreamProvider(payload)
                    .build()).call();
            String responseBody;
            if (response.responseBody().isPresent()) {
                try (var input = response.responseBody().get()) {
                    responseBody = new String(input.readAllBytes(), StandardCharsets.UTF_8);
                }
            } else {
                responseBody = "";
            }
            if (response.httpResponse().statusCode() != 201) {
                throw new RuntimeException("Failed to add table through REST API, status code: "
                        + response.httpResponse().statusCode() + ", response: " + responseBody);
            }
            return new AddTableResponseSerDe().fromJson(responseBody);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static URI addTableUri(InstanceProperties instanceProperties) {
        String restApiUrl = instanceProperties.get(REST_API_URL);
        return URI.create(restApiUrl + (restApiUrl.endsWith("/") ? "" : "/") + "sleeper/tables");
    }

    @Override
    public void close() {
        httpClient.close();
    }
}

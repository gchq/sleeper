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
package sleeper.clients.testutil;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;
import software.amazon.awssdk.services.cloudwatchevents.CloudWatchEventsClient;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient;

import java.net.URI;
import java.net.URISyntaxException;

import static sleeper.task.common.WiremockTestHelper.WIREMOCK_ACCESS_KEY;
import static sleeper.task.common.WiremockTestHelper.WIREMOCK_SECRET_KEY;
import static sleeper.task.common.WiremockTestHelper.wiremockAwsV2Client;
import static sleeper.task.common.WiremockTestHelper.wiremockCredentialsProvider;
import static sleeper.task.common.WiremockTestHelper.wiremockEndpointConfiguration;

public class ClientWiremockTestHelper {

    public static final String OPERATION_HEADER = "X-Amz-Target";

    private ClientWiremockTestHelper() {
    }

    public static EcrClient wiremockEcrClient(WireMockRuntimeInfo runtimeInfo) {
        return wiremockAwsV2Client(runtimeInfo, EcrClient.builder());
    }

    public static CloudWatchEventsClient wiremockCloudWatchClient(WireMockRuntimeInfo runtimeInfo) {
        return wiremockAwsV2Client(runtimeInfo, CloudWatchEventsClient.builder());
    }

    public static AmazonElasticMapReduce wiremockEmrClient(WireMockRuntimeInfo runtimeInfo) {
        return AmazonElasticMapReduceClient.builder()
                .withEndpointConfiguration(wiremockEndpointConfiguration(runtimeInfo))
                .withCredentials(wiremockCredentialsProvider())
                .build();
    }

    public static EmrServerlessClient wiremockEmrServerlessClient(WireMockRuntimeInfo runtimeInfo) {
        return callWiremock(EmrServerlessClient.builder(), runtimeInfo);
    }

    public static CloudWatchLogsClient wiremockLogsClient(WireMockRuntimeInfo runtimeInfo) {
        return callWiremock(CloudWatchLogsClient.builder(), runtimeInfo);
    }

    public static CloudFormationClient wiremockCloudFormationClient(WireMockRuntimeInfo runtimeInfo) {
        return callWiremock(CloudFormationClient.builder(), runtimeInfo);
    }

    public static <B extends AwsClientBuilder<B, T>, T> T callWiremock(
            B builder, WireMockRuntimeInfo runtimeInfo) {
        return builder
                .endpointOverride(wiremockEndpointOverride(runtimeInfo))
                .credentialsProvider(wiremockCredentialsProviderV2())
                .region(Region.AWS_GLOBAL)
                .build();
    }

    public static URI wiremockEndpointOverride(WireMockRuntimeInfo runtimeInfo) {
        try {
            return new URI(runtimeInfo.getHttpBaseUrl());
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    public static AwsCredentialsProvider wiremockCredentialsProviderV2() {
        return StaticCredentialsProvider.create(AwsBasicCredentials.create(WIREMOCK_ACCESS_KEY, WIREMOCK_SECRET_KEY));
    }
}

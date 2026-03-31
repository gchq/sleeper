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
package sleeper.core.util;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sts.StsClient;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.localstack.test.WiremockAwsV2ClientHelper.wiremockAwsV2Client;

@WireMockTest
public class S3BucketNameTest {

    private StsClient stsClient;

    @BeforeEach
    void setUp(WireMockRuntimeInfo runtimeInfo) {
        stsClient = wiremockAwsV2Client(runtimeInfo, StsClient.builder());
    }

    @Test
    void shouldRefuseBucketNameWithMoreThan63Characters() {

        // Given
        // 1. Arrange - Define the mock STS XML response
        String mockAccountId = "123456789012";
        stubFor(post(urlEqualTo("/"))
                .withHeader("Content-Type", containing("application/x-www-form-urlencoded"))
                .withRequestBody(containing("Action=GetCallerIdentity"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "text/xml")
                        .withBody(
                                "<GetCallerIdentityResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">" +
                                        "  <GetCallerIdentityResult>" +
                                        "    <Account>" + mockAccountId + "</Account>" +
                                        "    <Arn>arn:aws:iam::" + mockAccountId + ":user/MockUser</Arn>" +
                                        "    <UserId>AKIAI44QH8DHBEXAMPLE</UserId>" +
                                        "  </GetCallerIdentityResult>" +
                                        "  <ResponseMetadata><RequestId>test-id</RequestId></ResponseMetadata>" +
                                        "</GetCallerIdentityResponse>")));

        // When / Then
        assertThatThrownBy(() -> S3BucketName.parse("123456789", "thisisaverylongbucketnameandwillexceed63characters", stsClient))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Bucket name exceeds 63 characters.");
    }

}

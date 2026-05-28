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

package sleeper.systemtest.drivers.util;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

public class EksAuthTokenGeneratorTest {

    private static final AwsCredentialsProvider CREDENTIALS = StaticCredentialsProvider.create(
            AwsBasicCredentials.create("AKIDEXAMPLE", "EXAMPLEKEY"));

    @Test
    void shouldBindTokenToClusterByIncludingItInTheSignedHeaders() {
        // When
        String token = EksAuthTokenGenerator.generateToken("my-cluster", Region.EU_WEST_2, CREDENTIALS);

        // Then
        assertThat(token).startsWith("k8s-aws-v1.");
        String rawQuery = decodePresignedUrl(token).getRawQuery();
        assertThat(rawQuery).containsPattern("X-Amz-SignedHeaders=[^&]*x-k8s-aws-id");
    }

    @Test
    void shouldTargetStsInTheGivenRegion() {
        // When
        String token = EksAuthTokenGenerator.generateToken("my-cluster", Region.EU_WEST_2, CREDENTIALS);

        // Then
        URI url = decodePresignedUrl(token);
        assertThat(url.getScheme()).isEqualTo("https");
        assertThat(url.getHost()).isEqualTo("sts.eu-west-2.amazonaws.com");
    }

    private static URI decodePresignedUrl(String token) {
        byte[] decoded = Base64.getUrlDecoder().decode(token.substring("k8s-aws-v1.".length()));
        return URI.create(new String(decoded, StandardCharsets.UTF_8));
    }
}

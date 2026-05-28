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

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;
import software.amazon.awssdk.http.auth.spi.internal.signer.DefaultSignRequest;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;
import software.amazon.awssdk.regions.PartitionMetadata;
import software.amazon.awssdk.regions.Region;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;

/**
 * Generates an EKS bearer token by presigning an STS GetCallerIdentity request bound to a specific cluster. The token
 * has the form produced by aws-iam-authenticator: "k8s-aws-v1." followed by the base64url-encoded presigned URL. The
 * x-k8s-aws-id header is part of the signed canonical request, which is what binds the token to a specific cluster.
 */
public class EksAuthTokenGenerator {

    private EksAuthTokenGenerator() {
    }

    public static String generateToken(String clusterName, Region region, AwsCredentialsProvider credentialsProvider) {
        PartitionMetadata partitionMetadata = PartitionMetadata.of(region);
        SdkHttpRequest request = SdkHttpRequest.builder()
                .method(SdkHttpMethod.GET)
                .uri(URI.create("https://sts." + region.id() + "." + partitionMetadata.dnsSuffix() + "/"))
                .appendRawQueryParameter("Action", "GetCallerIdentity")
                .appendRawQueryParameter("Version", "2011-06-15")
                .putHeader("x-k8s-aws-id", clusterName)
                .build();
        SignedRequest signed = AwsV4HttpSigner.create().sign(DefaultSignRequest.builder(credentialsProvider.resolveCredentials())
                .putProperty(AwsV4HttpSigner.SERVICE_SIGNING_NAME, "sts")
                .putProperty(AwsV4HttpSigner.REGION_NAME, region.id())
                .putProperty(AwsV4HttpSigner.AUTH_LOCATION, AwsV4HttpSigner.AuthLocation.QUERY_STRING)
                .putProperty(AwsV4HttpSigner.EXPIRATION_DURATION, Duration.ofSeconds(60))
                .request(request)
                .build());
        String presignedUrl = signed.request().getUri().toString();
        return "k8s-aws-v1." + Base64.getUrlEncoder().withoutPadding()
                .encodeToString(presignedUrl.getBytes(StandardCharsets.UTF_8));
    }
}

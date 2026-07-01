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
package sleeper.parquet;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.s3a.S3ClientFactory;
import org.apache.hadoop.fs.s3a.impl.AWSClientConfig;
import org.apache.hadoop.fs.s3a.statistics.impl.AwsStatisticsCollector;
import software.amazon.awssdk.awscore.util.AwsHostNameUtils;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.auth.spi.scheme.AuthScheme;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3BaseClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.multipart.MultipartConfiguration;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_DEFAULT_REGION;
import static org.apache.hadoop.fs.s3a.Constants.AWS_SERVICE_IDENTIFIER_S3;
import static org.apache.hadoop.fs.s3a.Constants.CENTRAL_ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_SECURE_CONNECTIONS;
import static org.apache.hadoop.fs.s3a.Constants.HTTP_SIGNER_CLASS_NAME;
import static org.apache.hadoop.fs.s3a.Constants.HTTP_SIGNER_ENABLED;
import static org.apache.hadoop.fs.s3a.Constants.HTTP_SIGNER_ENABLED_DEFAULT;
import static org.apache.hadoop.fs.s3a.Constants.SECURE_CONNECTIONS;
import static org.apache.hadoop.fs.s3a.auth.SignerFactory.createHttpSigner;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.AUTH_SCHEME_AWS_SIGV_4;

public class HadoopS3ClientFactory extends Configured implements S3ClientFactory {

    private static final String S3_SERVICE_NAME = "s3";

    private static final Pattern VPC_ENDPOINT_PATTERN = Pattern.compile("^(?:.+\\.)?([a-z0-9-]+)\\.vpce\\.amazonaws\\.(?:com|com\\.cn)$");

    public HadoopS3ClientFactory() {
    }

    public static void configureHadoop(Configuration conf) {
        conf.set("fs.s3a.s3.client.factory.impl", HadoopS3ClientFactory.class.getName());
    }

    @Override
    public S3Client createS3Client(URI uri, S3ClientCreationParameters params) throws IOException {
        return configureClientBuilder(S3Client.builder(), params, getConf(), uri.getHost())
                .build();
    }

    @SuppressWarnings("unchecked")
    private <BuilderT extends S3BaseClientBuilder<BuilderT, ClientT>, ClientT> BuilderT configureClientBuilder(
            BuilderT builder, S3ClientCreationParameters parameters, Configuration conf, String bucket) throws IOException {

        configureEndpointAndRegion(builder, parameters, conf);

        S3Configuration serviceConfiguration = S3Configuration.builder()
                .pathStyleAccessEnabled(parameters.isPathStyleAccess())
                .build();

        final ClientOverrideConfiguration.Builder override = createClientOverrideConfiguration(parameters, conf);

        S3BaseClientBuilder s3BaseClientBuilder = builder
                .overrideConfiguration(override.build())
                .credentialsProvider(parameters.getCredentialSet())
                .disableS3ExpressSessionAuth(!parameters.isExpressCreateSession())
                .serviceConfiguration(serviceConfiguration);

        if (conf.getBoolean(HTTP_SIGNER_ENABLED, HTTP_SIGNER_ENABLED_DEFAULT)) {
            // use an http signer through an AuthScheme
            final AuthScheme<AwsCredentialsIdentity> signer = createHttpSigner(conf, AUTH_SCHEME_AWS_SIGV_4, HTTP_SIGNER_CLASS_NAME);
            builder.putAuthScheme(signer);
        }
        return (BuilderT) s3BaseClientBuilder;
    }

    private <BuilderT extends S3BaseClientBuilder<BuilderT, ClientT>, ClientT> void configureEndpointAndRegion(
            BuilderT builder, S3ClientCreationParameters parameters, Configuration conf) {
        final String endpointStr = parameters.getEndpoint();
        final URI endpoint = getS3Endpoint(endpointStr, conf);

        final String configuredRegion = parameters.getRegion();
        Region region = null;

        // If the region was configured, set it.
        if (configuredRegion != null && !configuredRegion.isEmpty()) {
            region = Region.of(configuredRegion);
        }

        if (endpoint != null) {
            boolean endpointEndsWithCentral = endpointStr.endsWith(CENTRAL_ENDPOINT);

            // No region was configured,
            // determine the region from the endpoint.
            if (region == null) {
                region = getS3RegionFromEndpoint(endpointStr,
                        endpointEndsWithCentral);
            }

            // No need to override endpoint with "s3.amazonaws.com".
            // Let the client take care of endpoint resolution. Overriding
            // the endpoint with "s3.amazonaws.com" causes 400 Bad Request
            // errors for non-existent buckets and objects.
            // ref: https://github.com/aws/aws-sdk-java-v2/issues/4846
            if (!endpointEndsWithCentral) {
                builder.endpointOverride(endpoint);
            } else {
                builder.crossRegionAccessEnabled(true);
            }
        }

        if (region != null) {
            builder.region(region);
        } else if (configuredRegion == null) {
            // no region is configured, and none could be determined from the endpoint.
            // Use EU_WEST_2 as default.
            region = Region.EU_WEST_2;
            builder.crossRegionAccessEnabled(true);
            builder.region(region);
        }
    }

    private static URI getS3Endpoint(String endpoint, final Configuration conf) {

        boolean secureConnections = conf.getBoolean(SECURE_CONNECTIONS, DEFAULT_SECURE_CONNECTIONS);

        String protocol = secureConnections ? "https" : "http";

        if (endpoint == null || endpoint.isEmpty()) {
            // don't set an endpoint if none is configured, instead let the SDK figure it out.
            return null;
        }

        if (!endpoint.contains("://")) {
            endpoint = String.format("%s://%s", protocol, endpoint);
        }

        try {
            return new URI(endpoint);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    static Region getS3RegionFromEndpoint(final String endpoint,
            final boolean endpointEndsWithCentral) {

        if (!endpointEndsWithCentral) {
            // S3 VPC endpoint parsing
            Matcher matcher = VPC_ENDPOINT_PATTERN.matcher(endpoint);
            if (matcher.find()) {
                return Region.of(matcher.group(1));
            }

            return AwsHostNameUtils.parseSigningRegion(endpoint, S3_SERVICE_NAME).orElse(null);
        }

        // Select default region here to enable cross-region access.
        // If both "fs.s3a.endpoint" and "fs.s3a.endpoint.region" are empty,
        // Spark sets "fs.s3a.endpoint" to "s3.amazonaws.com".
        // This applies to Spark versions with the changes of SPARK-35878.
        // ref:
        // https://github.com/apache/spark/blob/v3.5.0/core/
        // src/main/scala/org/apache/spark/deploy/SparkHadoopUtil.scala#L528
        // If we do not allow cross region access, Spark would not be able to
        // access any bucket that is not present in the given region.
        // Hence, we should use default region us-east-2 to allow cross-region
        // access.
        return Region.of(AWS_S3_DEFAULT_REGION);
    }

    protected ClientOverrideConfiguration.Builder createClientOverrideConfiguration(
            S3ClientCreationParameters parameters, Configuration conf) throws IOException {
        final ClientOverrideConfiguration.Builder clientOverrideConfigBuilder = AWSClientConfig.createClientConfigBuilder(conf, AWS_SERVICE_IDENTIFIER_S3);

        // add any headers
        parameters.getHeaders().forEach((h, v) -> clientOverrideConfigBuilder.putHeader(h, v));

        if (!StringUtils.isEmpty(parameters.getUserAgentSuffix())) {
            clientOverrideConfigBuilder.putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_SUFFIX,
                    parameters.getUserAgentSuffix());
        }

        if (parameters.getExecutionInterceptors() != null) {
            for (ExecutionInterceptor interceptor : parameters.getExecutionInterceptors()) {
                clientOverrideConfigBuilder.addExecutionInterceptor(interceptor);
            }
        }

        if (parameters.getMetrics() != null) {
            clientOverrideConfigBuilder.addMetricPublisher(
                    new AwsStatisticsCollector(parameters.getMetrics()));
        }

        return clientOverrideConfigBuilder;
    }

    @Override
    public S3AsyncClient createS3AsyncClient(
            final URI uri,
            final S3ClientCreationParameters parameters) throws IOException {

        Configuration conf = getConf();
        String bucket = uri.getHost();

        MultipartConfiguration multipartConfiguration = MultipartConfiguration.builder()
                .minimumPartSizeInBytes(parameters.getMinimumPartSize())
                .thresholdInBytes(parameters.getMultiPartThreshold())
                .build();

        return configureClientBuilder(S3AsyncClient.builder(), parameters, conf, bucket)
                .multipartConfiguration(multipartConfiguration)
                .multipartEnabled(parameters.isMultipartCopy())
                .build();
    }

    @Override
    public S3TransferManager createS3TransferManager(S3AsyncClient s3AsyncClient) {
        return S3TransferManager.builder()
                .s3Client(s3AsyncClient)
                .build();
    }
}

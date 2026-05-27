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
package sleeper.foreign.datafusion;

import sleeper.core.properties.instance.InstanceProperties;

/**
 * AWS configuration overrides to pass to Rust DataFusion code.
 */
public class DataFusionAwsConfig {

    private final String region;
    private final String endpoint;
    private final String accessKeyId;
    private final String secretAccessKey;
    private final String sessionToken;
    private final boolean allowHttp;

    /**
     * Creates a DataFusionAwsConfig.
     *
     * All fields be null, however secretAccessKey and accessKeyId must either both be null or non-null.
     *
     * @param  builder                  builder object
     * @throws IllegalArgumentException if only one of secretAccessKey and accessKeyId are null
     */
    private DataFusionAwsConfig(Builder builder) {
        if (builder.secretAccessKey == null && builder.accessKeyId != null ||
                builder.secretAccessKey != null && builder.accessKeyId == null) {
            throw new IllegalArgumentException("secretAccessKey and accessKeyId must either both be null or non-null");
        }
        region = builder.region;
        endpoint = builder.endpoint;
        accessKeyId = builder.accessKeyId;
        secretAccessKey = builder.secretAccessKey;
        sessionToken = builder.sessionToken;
        allowHttp = builder.allowHttp;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates the default AWS configuration. Applies configuration from environment
     * variables if set.
     *
     * @param  instanceProperties Sleeper instance properties
     * @return                    AWS config for DataFusion
     */
    public static DataFusionAwsConfig getDefault(InstanceProperties instanceProperties) {
        String endpoint = System.getenv("AWS_ENDPOINT_URL");
        if (endpoint != null) {
            return overrideEndpoint(endpoint);
        } else {
            return builder()
                    .endpoint(instanceProperties.evaluateAWSEndpoint("s3"))
                    .build();
        }
    }

    /**
     * Creates a configuration to run against a LocalStack endpoint.
     *
     * Reads an endpoint URL from the environment if "AWS_ENDPOINT_URL" if endpoint argument
     * is null. If both are null, this method returns null.
     *
     * @param  endpoint the endpoint to use
     * @return          the configuration
     */
    public static DataFusionAwsConfig overrideEndpoint(String endpoint) {
        return builder()
                .endpoint(endpoint)
                .region("us-east-1")
                .accessKeyId("test-access-key-id")
                .secretAccessKey("test-secret-access-key")
                .allowHttp(true)
                .build();
    }

    public String getRegion() {
        return region;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public String getSecretAccessKey() {
        return secretAccessKey;
    }

    public String getSessionToken() {
        return sessionToken;
    }

    public boolean isAllowHttp() {
        return allowHttp;
    }

    /**
     * Converts this configuration to an FFI struct to be passed to DataFusion.
     *
     * @param  runtime the FFI runtime
     * @return         the struct
     */
    public FFIAwsConfig toFfi(jnr.ffi.Runtime runtime) {
        FFIAwsConfig config = new FFIAwsConfig(runtime);
        config.region.set(region);
        config.endpoint.set(endpoint);
        config.access_key_id.set(accessKeyId);
        config.secret_access_key.set(secretAccessKey);
        config.session_token.set(sessionToken);
        config.allow_http.set(allowHttp);
        return config;
    }

    /**
     * Builder for DataFusion AWS configuration objects.
     */
    public static class Builder {
        private String region;
        private String endpoint;
        private String accessKeyId;
        private String secretAccessKey;
        private String sessionToken;
        private boolean allowHttp;

        private Builder() {
        }

        /**
         * Sets the AWS region.
         *
         * @param  region the region
         * @return        the builder for chaining
         */
        public Builder region(String region) {
            this.region = region;
            return this;
        }

        /**
         * Sets the AWS endpoint.
         *
         * @param  endpoint the endpoint
         * @return          the builder for chaining
         */
        public Builder endpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        /**
         * Sets the access key ID.
         *
         * @param  accessKeyId the access key ID
         * @return             the builder for chaining
         */
        public Builder accessKeyId(String accessKeyId) {
            this.accessKeyId = accessKeyId;
            return this;
        }

        /**
         * Sets the secret access key.
         *
         * @param  secretAccessKey the secret key
         * @return                 the builder for chaining
         */
        public Builder secretAccessKey(String secretAccessKey) {
            this.secretAccessKey = secretAccessKey;
            return this;
        }

        /**
         * Sets the session token.
         *
         * @param  sessionToken the session token
         * @return              the builder for chaining
         */
        public Builder sessionToken(String sessionToken) {
            this.sessionToken = sessionToken;
            return this;
        }

        /**
         * Sets whether or not an AWS client should allow connecting with HTTP instead of HTTPS.
         *
         * @param  allowHttp true if HTTP should be allowed
         * @return           the builder for chaining
         */
        public Builder allowHttp(boolean allowHttp) {
            this.allowHttp = allowHttp;
            return this;
        }

        public DataFusionAwsConfig build() {
            return new DataFusionAwsConfig(this);
        }
    }
}

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
package sleeper.foreign.datafusion;

/**
 * AWS configuration overrides to pass to Rust DataFusion code.
 */
public class DataFusionAwsConfig {

    private final String region;
    private final String endpoint;
    private final String accessKey;
    private final String secretKey;
    private final boolean allowHttp;

    private DataFusionAwsConfig(Builder builder) {
        region = builder.region;
        endpoint = builder.endpoint;
        accessKey = builder.accessKey;
        secretKey = builder.secretKey;
        allowHttp = builder.allowHttp;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates the default AWS configuration. Usually this will be null. Applies configuration from environment
     * variables if set.
     *
     * @return the configuration, if set
     */
    public static DataFusionAwsConfig getDefault() {
        String endpoint = System.getenv("AWS_ENDPOINT_URL");
        if (endpoint != null) {
            return overrideEndpoint(endpoint);
        } else {
            return null;
        }
    }

    /**
     * Creates a configuration to run against a LocalStack endpoint.
     *
     * @param  endpoint the endpoint
     * @return          the configuration
     */
    public static DataFusionAwsConfig overrideEndpoint(String endpoint) {
        return builder()
                .endpoint(endpoint)
                .region("us-east-1")
                .accessKey("test-access-key")
                .secretKey("test-secret-key")
                .allowHttp(true)
                .build();
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
        config.endpoint.set(endpoint == null ? "" : endpoint);
        config.access_key.set(accessKey);
        config.secret_key.set(secretKey);
        config.allow_http.set(allowHttp);
        return config;
    }

    /**
     * Builder for DataFusion AWS configuration objects.
     */
    public static class Builder {
        private String region;
        private String endpoint;
        private String accessKey;
        private String secretKey;
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
         * Sets the access key.
         *
         * @param  accessKey the access key
         * @return           the builder for chaining
         */
        public Builder accessKey(String accessKey) {
            this.accessKey = accessKey;
            return this;
        }

        /**
         * Sets the secret key.
         *
         * @param  secretKey the secret key
         * @return           the builder for chaining
         */
        public Builder secretKey(String secretKey) {
            this.secretKey = secretKey;
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

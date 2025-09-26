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

import jnr.ffi.Struct;

import java.util.Objects;

/**
 * The AWS configuration to be supplied to DataFusion.
 *
 * <strong>THIS IS A C COMPATIBLE FFI STRUCT!</strong> If you updated this struct (field ordering, types, etc.),
 * you MUST update the corresponding Rust definition in rust/sleeper_df/src/objects.rs. The order and types of
 * the fields must match exactly.
 */
@SuppressWarnings(value = "checkstyle:memberName")
public class FFIAwsConfig extends Struct {
    public final Struct.UTF8StringRef region = new Struct.UTF8StringRef();
    public final Struct.UTF8StringRef endpoint = new Struct.UTF8StringRef();
    public final Struct.UTF8StringRef access_key = new Struct.UTF8StringRef();
    public final Struct.UTF8StringRef secret_key = new Struct.UTF8StringRef();
    public final Struct.Boolean allow_http = new Struct.Boolean();

    public FFIAwsConfig(jnr.ffi.Runtime runtime) {
        super(runtime);
        region.set("");
        endpoint.set("");
        access_key.set("");
        secret_key.set("");
    }

    public java.lang.String getRegion() {
        return region.get();
    }

    public java.lang.String getEndpoint() {
        return endpoint.get();
    }

    public java.lang.String getAccessKey() {
        return access_key.get();
    }

    public java.lang.String getSecretKey() {
        return secret_key.get();
    }

    public boolean isAllowHttp() {
        return allow_http.get();
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates the default AWS configuration. Usually this will be null. Applies configuration from environment
     * variables if set.
     *
     * @param  runtime the JNR runtime
     * @return         the configuration, if set
     */
    public static FFIAwsConfig getDefault(jnr.ffi.Runtime runtime) {
        java.lang.String endpoint = System.getenv("AWS_ENDPOINT_URL");
        if (endpoint != null) {
            return overrideEndpoint(runtime, endpoint);
        } else {
            return null;
        }
    }

    /**
     * Creates a configuration to run against a LocalStack endpoint.
     *
     * @param  runtime  the JNR runtime
     * @param  endpoint the endpoint
     * @return          the configuration
     */
    public static FFIAwsConfig overrideEndpoint(jnr.ffi.Runtime runtime, java.lang.String endpoint) {
        return builder()
                .runtime(runtime)
                .endpoint(endpoint)
                .region("us-east-1")
                .accessKey("test-access-key")
                .secretKey("test-secret-key")
                .allowHttp(true)
                .build();
    }

    /**
     * Builder for DataFusion AWS configuration objects.
     */
    public static class Builder {
        private jnr.ffi.Runtime runtime;
        private java.lang.String region;
        private java.lang.String endpoint;
        private java.lang.String accessKey;
        private java.lang.String secretKey;
        private boolean allowHttp;

        private Builder() {
        }

        /**
         * Sets the JNR runtime.
         *
         * @param  runtime the runtime
         * @return         the builder for chaining
         */
        public Builder runtime(jnr.ffi.Runtime runtime) {
            this.runtime = runtime;
            return this;
        }

        /**
         * Sets the AWS region.
         *
         * @param  region the region
         * @return        the builder for chaining
         */
        public Builder region(java.lang.String region) {
            this.region = region;
            return this;
        }

        /**
         * Sets the AWS endpoint.
         *
         * @param  endpoint the endpoint
         * @return          the builder for chaining
         */
        public Builder endpoint(java.lang.String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        /**
         * Sets the access key.
         *
         * @param  accessKey the access key
         * @return           the builder for chaining
         */
        public Builder accessKey(java.lang.String accessKey) {
            this.accessKey = accessKey;
            return this;
        }

        /**
         * Sets the secret key.
         *
         * @param  secretKey the secret key
         * @return           the builder for chaining
         */
        public Builder secretKey(java.lang.String secretKey) {
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

        /**
         * Build the completed object.
         *
         * @return                      FFI compatible AWS config object
         * @throws NullPointerException if any parameter in the builder is {@code null}
         */
        public FFIAwsConfig build() {
            FFIAwsConfig ffiAwsConfig = new FFIAwsConfig(Objects.requireNonNull(runtime, "runtime"));
            ffiAwsConfig.region.set(Objects.requireNonNull(region, "region"));
            ffiAwsConfig.endpoint.set(Objects.requireNonNull(endpoint, "endpoint"));
            ffiAwsConfig.allow_http.set(allowHttp);
            ffiAwsConfig.access_key.set(Objects.requireNonNull(accessKey, "accessKey"));
            ffiAwsConfig.secret_key.set(Objects.requireNonNull(secretKey, "secretKey"));
            return ffiAwsConfig;
        }
    }
}

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
package sleeper.compaction.datafusion;

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

    public static DataFusionAwsConfig getDefault() {
        String endpoint = System.getenv("AWS_ENDPOINT_URL");
        if (endpoint != null) {
            return overrideEndpoint(endpoint);
        } else {
            return null;
        }
    }

    public static DataFusionAwsConfig overrideEndpoint(String endpoint) {
        return builder()
                .endpoint(endpoint)
                .region("us-east-1")
                .accessKey("test-access-key")
                .secretKey("test-secret-key")
                .allowHttp(true)
                .build();
    }

    public String getRegion() {
        return region;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public boolean isAllowHttp() {
        return allowHttp;
    }

    public static class Builder {
        private String region;
        private String endpoint;
        private String accessKey;
        private String secretKey;
        private boolean allowHttp;

        private Builder() {
        }

        public Builder region(String region) {
            this.region = region;
            return this;
        }

        public Builder endpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public Builder accessKey(String accessKey) {
            this.accessKey = accessKey;
            return this;
        }

        public Builder secretKey(String secretKey) {
            this.secretKey = secretKey;
            return this;
        }

        public Builder allowHttp(boolean allowHttp) {
            this.allowHttp = allowHttp;
            return this;
        }

        public DataFusionAwsConfig build() {
            return new DataFusionAwsConfig(this);
        }
    }
}

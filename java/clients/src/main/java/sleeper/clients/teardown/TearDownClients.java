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

package sleeper.clients.teardown;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;
import software.amazon.awssdk.services.cloudwatchevents.CloudWatchEventsClient;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.ecs.EcsClient;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.util.Objects;

public class TearDownClients {

    private final AmazonS3 s3;
    private final S3Client s3v2;
    private final CloudWatchEventsClient cloudWatch;
    private final EcsClient ecs;
    private final EcrClient ecr;
    private final EmrClient emr;
    private final EmrServerlessClient emrServerless;
    private final CloudFormationClient cloudFormation;

    private TearDownClients(Builder builder) {
        s3 = Objects.requireNonNull(builder.s3, "s3 must not be null");
        s3v2 = Objects.requireNonNull(builder.s3v2, "s3v2 must not be null");
        cloudWatch = Objects.requireNonNull(builder.cloudWatch, "cloudWatch must not be null");
        ecs = Objects.requireNonNull(builder.ecs, "ecs must not be null");
        ecr = Objects.requireNonNull(builder.ecr, "ecr must not be null");
        emr = Objects.requireNonNull(builder.emr, "emr must not be null");
        emrServerless = Objects.requireNonNull(builder.emrServerless, "emrServerless must not be null");
        cloudFormation = Objects.requireNonNull(builder.cloudFormation, "cloudFormation must not be null");
    }

    public static void withDefaults(TearDownOperation operation) throws IOException, InterruptedException {
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        try (S3Client s3v2Client = S3Client.create();
                CloudWatchEventsClient cloudWatchClient = CloudWatchEventsClient.create();
                EcrClient ecrClient = EcrClient.create();
                EcsClient ecsClient = EcsClient.create();
                EmrClient emrClient = EmrClient.create();
                EmrServerlessClient emrServerless = EmrServerlessClient.create();
                CloudFormationClient cloudFormationClient = CloudFormationClient.create()) {
            TearDownClients clients = builder()
                    .s3(s3Client)
                    .s3v2(s3v2Client)
                    .cloudWatch(cloudWatchClient)
                    .ecs(ecsClient)
                    .ecr(ecrClient)
                    .emr(emrClient)
                    .emrServerless(emrServerless)
                    .cloudFormation(cloudFormationClient)
                    .build();
            operation.tearDown(clients);
        } finally {
            s3Client.shutdown();
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public AmazonS3 getS3() {
        return s3;
    }

    public S3Client getS3v2() {
        return s3v2;
    }

    public CloudWatchEventsClient getCloudWatch() {
        return cloudWatch;
    }

    public EcsClient getEcs() {
        return ecs;
    }

    public EcrClient getEcr() {
        return ecr;
    }

    public EmrClient getEmr() {
        return emr;
    }

    public EmrServerlessClient getEmrServerless() {
        return emrServerless;
    }

    public CloudFormationClient getCloudFormation() {
        return cloudFormation;
    }

    public static final class Builder {
        private AmazonS3 s3;
        private S3Client s3v2;
        private CloudWatchEventsClient cloudWatch;
        private EcsClient ecs;
        private EcrClient ecr;
        private EmrClient emr;
        private EmrServerlessClient emrServerless;
        private CloudFormationClient cloudFormation;

        private Builder() {
        }

        public Builder s3(AmazonS3 s3) {
            this.s3 = s3;
            return this;
        }

        public Builder s3v2(S3Client s3v2) {
            this.s3v2 = s3v2;
            return this;
        }

        public Builder cloudWatch(CloudWatchEventsClient cloudWatch) {
            this.cloudWatch = cloudWatch;
            return this;
        }

        public Builder ecs(EcsClient ecs) {
            this.ecs = ecs;
            return this;
        }

        public Builder ecr(EcrClient ecr) {
            this.ecr = ecr;
            return this;
        }

        public Builder emr(EmrClient emr) {
            this.emr = emr;
            return this;
        }

        public Builder emrServerless(EmrServerlessClient emrServerless) {
            this.emrServerless = emrServerless;
            return this;
        }

        public Builder cloudFormation(CloudFormationClient cloudFormation) {
            this.cloudFormation = cloudFormation;
            return this;
        }

        public TearDownClients build() {
            return new TearDownClients(this);
        }
    }

    public interface TearDownOperation {
        void tearDown(TearDownClients clients) throws IOException, InterruptedException;
    }
}

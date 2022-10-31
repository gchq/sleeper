/*
 * Copyright 2022 Crown Copyright
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

package sleeper.ingest.job;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.PutMetricDataResult;

public class AWSCloudWatchReporter implements CloudWatchReporter {
    private final AmazonCloudWatch cloudWatchClient;

    private AWSCloudWatchReporter(Builder builder) {
        cloudWatchClient = builder.cloudWatchClient;
    }

    public static AWSCloudWatchReporter from(AmazonCloudWatch cloudWatchClient) {
        return builder().cloudWatchClient(cloudWatchClient).build();
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean report(PutMetricDataRequest metricData) {
        PutMetricDataResult result = cloudWatchClient.putMetricData(metricData);
        return result.getSdkHttpMetadata().getHttpStatusCode() == 200;
    }

    public static final class Builder {
        private AmazonCloudWatch cloudWatchClient;

        private Builder() {
        }

        public Builder cloudWatchClient(AmazonCloudWatch cloudWatchClient) {
            this.cloudWatchClient = cloudWatchClient;
            return this;
        }

        public AWSCloudWatchReporter build() {
            return new AWSCloudWatchReporter(this);
        }
    }
}

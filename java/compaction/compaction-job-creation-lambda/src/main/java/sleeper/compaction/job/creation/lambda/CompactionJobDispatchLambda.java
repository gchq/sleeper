/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.compaction.job.creation.lambda;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;
import org.apache.hadoop.conf.Configuration;

import sleeper.compaction.core.job.dispatch.CompactionJobDispatcher;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.statestore.StateStoreFactory;

import java.time.Instant;
import java.util.function.Supplier;

public class CompactionJobDispatchLambda {

    private CompactionJobDispatchLambda() {
    }

    public static CompactionJobDispatcher dispatcher(
            AmazonS3 s3, AmazonDynamoDB dynamoDB, Configuration conf, String configBucket, Supplier<Instant> timeSupplier) {
        InstanceProperties instanceProperties = S3InstanceProperties.loadFromBucket(s3, configBucket);
        StateStoreProvider stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3, dynamoDB, conf);
        return new CompactionJobDispatcher(instanceProperties,
                S3TableProperties.createProvider(instanceProperties, s3, dynamoDB),
                stateStoreProvider, null, null, null, timeSupplier);
    }

}

/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.cdk.stack;

import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.services.s3.BlockPublicAccess;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.BucketEncryption;
import software.constructs.Construct;

import sleeper.cdk.Utils;
import sleeper.configuration.properties.InstanceProperties;

import java.util.Locale;

import static sleeper.configuration.properties.CommonProperties.ID;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * This configuration stack deploys the config bucket used to store the Sleeper
 * properties.
 */
public class ConfigurationStack extends NestedStack {

    public ConfigurationStack(Construct scope, String id, InstanceProperties instanceProperties) {
        super(scope, id);

        Bucket configBucket = Bucket.Builder.create(this, "ConfigBucket")
                .bucketName(String.join("-", "sleeper", instanceProperties.get(ID), "config").toLowerCase(Locale.ROOT))
                .versioned(false)
                .encryption(BucketEncryption.S3_MANAGED)
                .blockPublicAccess(BlockPublicAccess.BLOCK_ALL)
                .removalPolicy(RemovalPolicy.DESTROY)
                .autoDeleteObjects(true)
                .build();

        instanceProperties.set(CONFIG_BUCKET, configBucket.getBucketName());

        Utils.addStackTagIfSet(this, instanceProperties);
    }
}

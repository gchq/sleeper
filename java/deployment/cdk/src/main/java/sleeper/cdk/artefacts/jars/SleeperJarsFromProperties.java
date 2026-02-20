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
package sleeper.cdk.artefacts.jars;

import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.constructs.Construct;

import sleeper.core.properties.instance.InstanceProperties;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;

/**
 * Creates references to jars based on the instance properties. This must use the same InstanceProperties object
 * that is passed to SleeperInstance.
 */
public class SleeperJarsFromProperties implements SleeperJars {

    private final InstanceProperties instanceProperties;
    private final SleeperJarVersionIdProvider versionIds;

    public SleeperJarsFromProperties(InstanceProperties instanceProperties, SleeperJarVersionIdProvider versionIds) {
        this.instanceProperties = instanceProperties;
        this.versionIds = versionIds;
    }

    @Override
    public SleeperLambdaJars lambdaJarsAtScope(Construct scope) {
        IBucket bucket = Bucket.fromBucketName(scope, "LambdaJarsBucket", instanceProperties.get(JARS_BUCKET));
        return jar -> Code.fromBucket(bucket, jar.getFilename(instanceProperties.get(VERSION)), versionIds.getLatestVersionId(jar));
    }

}

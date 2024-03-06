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
package sleeper.cdk.jars;

import com.amazonaws.services.s3.AmazonS3;
import software.amazon.awscdk.services.s3.IBucket;

import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.HashMap;
import java.util.Map;

import static sleeper.configuration.properties.instance.CommonProperty.JARS_BUCKET;

public class BuiltJars {

    private final AmazonS3 s3;
    private final String bucketName;
    private final LambdaBuilder.Configuration globalConfig;
    private final Map<String, String> jarFilenameToVersionId = new HashMap<>();

    public BuiltJars(AmazonS3 s3, InstanceProperties instanceProperties) {
        this(s3, instanceProperties.get(JARS_BUCKET), new GlobalLambdaConfiguration(instanceProperties));
    }

    public BuiltJars(AmazonS3 s3, String bucketName) {
        this(s3, bucketName, LambdaBuilder.Configuration.none());
    }

    private BuiltJars(AmazonS3 s3, String bucketName, LambdaBuilder.Configuration globalConfig) {
        this.s3 = s3;
        this.bucketName = bucketName;
        this.globalConfig = globalConfig;
    }

    public String bucketName() {
        return bucketName;
    }

    public LambdaCode lambdaCode(BuiltJar jar, IBucket bucketConstruct) {
        return new LambdaCode(bucketConstruct, jar.getFileName(), getLatestVersionId(jar), globalConfig);
    }

    public String getLatestVersionId(BuiltJar jar) {
        return jarFilenameToVersionId.computeIfAbsent(jar.getFileName(),
                filename -> s3.getObjectMetadata(bucketName, filename).getVersionId());
    }
}

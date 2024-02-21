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

public class BuiltJars {

    private final AmazonS3 s3;
    private final String bucketName;

    public BuiltJars(AmazonS3 s3, String bucketName) {
        this.s3 = s3;
        this.bucketName = bucketName;
    }

    public String bucketName() {
        return bucketName;
    }

    public LambdaCode lambdaCode(BuiltJar jar, IBucket bucketConstruct) {
        return new LambdaCode(bucketConstruct, jar.getFileName(), getLatestVersionId(jar));
    }

    public String getLatestVersionId(BuiltJar jar) {
        return s3.getObjectMetadata(bucketName, jar.getFileName()).getVersionId();
    }
}

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
package sleeper.systemtest.drivers.testutil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.systemtest.configurationv2.SystemTestStandaloneProperties;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.instance.SystemTestDeploymentDriver;
import sleeper.systemtest.dsl.instance.SystemTestParameters;

import static sleeper.systemtest.configurationv2.SystemTestProperty.SYSTEM_TEST_BUCKET_NAME;
import static sleeper.systemtest.configurationv2.SystemTestProperty.SYSTEM_TEST_ID;

public class LocalStackSystemTestDeploymentDriver implements SystemTestDeploymentDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalStackSystemTestDeploymentDriver.class);

    private final SystemTestParameters parameters;
    private final S3Client s3;

    public LocalStackSystemTestDeploymentDriver(SystemTestParameters parameters, SystemTestClients clients) {
        this.parameters = parameters;
        s3 = clients.getS3V2();
    }

    @Override
    public void saveProperties(SystemTestStandaloneProperties properties) {
        properties.saveToS3(s3);
    }

    @Override
    public SystemTestStandaloneProperties loadProperties() {
        return SystemTestStandaloneProperties.fromS3GivenDeploymentId(s3, parameters.getSystemTestShortId());
    }

    @Override
    public boolean deployIfNotPresent(SystemTestStandaloneProperties properties) {
        String deploymentId = properties.get(SYSTEM_TEST_ID);
        String bucketName = SystemTestStandaloneProperties.buildSystemTestBucketName(deploymentId);
        if (doesBucketExist(bucketName)) {
            LOGGER.info("Deployment already exists: {}", deploymentId);
            return false;
        } else {
            LOGGER.info("Creating system test deployment: {}", deploymentId);
            s3.createBucket(request -> request.bucket(bucketName));
            properties.set(SYSTEM_TEST_BUCKET_NAME, bucketName);
            properties.saveToS3(s3);
            return true;
        }
    }

    @Override
    public void redeploy(SystemTestStandaloneProperties properties) {
        throw new UnsupportedOperationException("Unimplemented method 'redeploy'");
    }

    private boolean doesBucketExist(String bucketName) {
        try {
            s3.getBucketAcl(request -> request.bucket(bucketName));
            return true;
        } catch (AwsServiceException e) {
            if (e.statusCode() == HttpStatusCode.NOT_FOUND) {
                return false;
            } else {
                throw e;
            }
        }
    }

}

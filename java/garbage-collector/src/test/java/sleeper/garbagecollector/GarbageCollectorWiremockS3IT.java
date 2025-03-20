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
package sleeper.garbagecollector;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import sleeper.localstack.test.WiremockAwsV1ClientHelper;

import java.time.Instant;

import static sleeper.garbagecollector.GarbageCollector.deleteFilesAndSketches;

@WireMockTest
public class GarbageCollectorWiremockS3IT extends GarbageCollectorTestBase {

    private AmazonS3 s3Client;

    @BeforeEach
    void setUp(WireMockRuntimeInfo runtimeInfo) {
        s3Client = WiremockAwsV1ClientHelper.buildAwsV1Client(runtimeInfo, AmazonS3ClientBuilder.standard());
    }

    @Test
    @Disabled
    void shouldTriggerRateLimitExceptionAndReduceRate() {

    }

    protected int collectGarbageAtTime(Instant time) throws Exception {
        return collectorWithDeleteAction(deleteFilesAndSketches(s3Client))
                .runAtTime(time, tables);
    }
}

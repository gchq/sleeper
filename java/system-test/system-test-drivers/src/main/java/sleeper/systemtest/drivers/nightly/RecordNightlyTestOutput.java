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

package sleeper.systemtest.drivers.nightly;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import java.io.IOException;
import java.nio.file.Path;

public class RecordNightlyTestOutput {

    private RecordNightlyTestOutput() {
    }

    public static void main(String[] args) throws IOException {
        String bucketName = args[0];
        NightlyTestTimestamp timestamp = NightlyTestTimestamp.from(args[1]);
        Path output = Path.of(args[2]);
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        try {
            NightlyTestOutput.from(output).uploadToS3(s3Client, bucketName, timestamp);
        } finally {
            s3Client.shutdown();
        }
    }
}

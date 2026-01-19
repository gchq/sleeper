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
package sleeper.configuration.utils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

public class S3PathTest {

    @Test
    public void shouldHandlePathWithoutPrefix() {
        //Given
        String path = "justBucket";

        //When
        S3Path s3Path = S3Path.parse(path);

        //Then
        assertThat(s3Path.requestedPath()).isEqualTo(path);
        assertThat(s3Path.bucket()).isEqualTo(path);
        assertThat(s3Path.pathInBucket()).isEqualTo("");
    }

    @Test
    public void shouldHandlePathWithPrefix() {
        //Given
        String path = "bucket/prefix";

        //When
        S3Path s3Path = S3Path.parse(path);

        //Then
        assertThat(s3Path.requestedPath()).isEqualTo(path);
        assertThat(s3Path.bucket()).isEqualTo("bucket");
        assertThat(s3Path.pathInBucket()).isEqualTo("prefix");
    }

    @Test
    public void shouldHandlePathWithPrefixAndDoubleSlash() {
        //Given
        String path = "bucket//prefix";

        //When
        S3Path s3Path = S3Path.parse(path);

        //Then
        assertThat(s3Path.requestedPath()).isEqualTo(path);
        assertThat(s3Path.bucket()).isEqualTo("bucket");
        assertThat(s3Path.pathInBucket()).isEqualTo("prefix");
    }

    @ParameterizedTest
    @CsvSource({"s3://bucket/prefix", "s3a://bucket/prefix"})
    public void shouldHandlePathWithScheme(String path) {
        //When
        S3Path s3Path = S3Path.parse(path);

        //Then
        assertThat(s3Path.requestedPath()).isEqualTo(path);
        assertThat(s3Path.bucket()).isEqualTo("bucket");
        assertThat(s3Path.pathInBucket()).isEqualTo("prefix");
    }
}

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

package sleeper.systemtest.drivers.nightly;

import java.nio.file.Path;
import java.util.Objects;

public class NightlyTestUploadFile {

    private final Path file;
    private final String relativeS3Key;

    private NightlyTestUploadFile(Path file, String s3Prefix) {
        this.file = file;
        this.relativeS3Key = s3Prefix + file.getFileName();
    }

    public static NightlyTestUploadFile fileInUploadDir(Path file) {
        return new NightlyTestUploadFile(file, "");
    }

    public static NightlyTestUploadFile fileInS3RelativeDir(String s3RelativeDir, Path file) {
        return new NightlyTestUploadFile(file, s3RelativeDir + "/");
    }

    public Path getFile() {
        return file;
    }

    public String getRelativeS3Key() {
        return relativeS3Key;
    }

    @Override
    public String toString() {
        return relativeS3Key + " from " + file;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        NightlyTestUploadFile that = (NightlyTestUploadFile) object;
        return Objects.equals(file, that.file) && Objects.equals(relativeS3Key, that.relativeS3Key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(file, relativeS3Key);
    }
}

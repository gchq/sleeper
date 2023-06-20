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
package sleeper.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility class of methods common to ingest jobs.
 */
public class HadoopPathUtils {

    private HadoopPathUtils() {
    }

    public static List<Path> getPaths(List<String> files, Configuration conf, String fileSystemProperty) {
        if (null == files || files.isEmpty()) {
            return Collections.emptyList();
        }

        return getFiles(files, conf, fileSystemProperty)
                .flatMap(status -> {
                    if (status.isDirectory()) {
                        try {
                            return getFilesOnPath(status.getPath(), conf, fileSystemProperty);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    } else {
                        if (!status.getPath().getName().endsWith(".crc")) {
                            return Stream.of(status);
                        }
                    }
                    return Stream.empty();
                })
                .map(FileStatus::getPath)
                .collect(Collectors.toList());
    }

    public static Stream<FileStatus> getFilesOnPath(Path path, Configuration conf, String fileSystemProperty) throws IOException {
        return getFiles(Arrays.stream(path.getFileSystem(conf).listStatus(path))
                .map(status -> status.getPath().toUri().getPath())
                .collect(Collectors.toList()), conf, fileSystemProperty);
    }

    public static Stream<FileStatus> getFiles(List<String> files, Configuration conf, String fileSystemProperty) {
        return files.stream()
                .map(file -> new Path(fileSystemProperty + file))
                .flatMap(path -> {
                    try {
                        return Stream.of(path.getFileSystem(conf).listStatus(path));
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
    }
}

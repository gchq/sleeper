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
package sleeper.io.parquet.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import sleeper.core.properties.instance.InstanceProperties;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;

/**
 * Utility class of methods common to ingest jobs.
 */
public class HadoopPathUtils {

    private HadoopPathUtils() {
    }

    public static List<String> expandDirectories(List<String> files, Configuration conf, InstanceProperties properties) {
        try {
            return streamFiles(files, conf, properties.get(FILE_SYSTEM))
                    .map(HadoopPathUtils::getRequestPath)
                    .collect(Collectors.toList());
        } catch (UncheckedIOException e) {
            return Collections.emptyList();
        }
    }

    public static List<Path> getPaths(List<String> files, Configuration conf, String fileSystemProperty) {
        return streamFiles(files, conf, fileSystemProperty)
                .map(FileStatus::getPath)
                .collect(Collectors.toList());
    }

    public static Stream<FileStatus> streamFiles(List<String> files, Configuration conf, String fileSystemProperty) {
        if (null == files || files.isEmpty()) {
            return Stream.empty();
        }
        return streamFiles(
                files.stream().map(file -> new Path(fileSystemProperty + file)),
                conf);
    }

    private static Stream<FileStatus> streamFiles(Stream<Path> paths, Configuration conf) {
        return paths.flatMap(path -> {
            try {
                return Stream.of(path.getFileSystem(conf).listStatus(path));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).flatMap(status -> {
            if (status.isDirectory()) {
                return streamFiles(Stream.of(status.getPath()), conf);
            } else if (!status.getPath().getName().endsWith(".crc")) {
                return Stream.of(status);
            } else {
                return Stream.empty();
            }
        });
    }

    public static String getRequestPath(FileStatus file) {
        URI uri = file.getPath().toUri();
        return uri.getHost() + uri.getPath();
    }
}

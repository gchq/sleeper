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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Utility class of methods common to ingest jobs.
 */
public class HadoopPathUtils {

    private HadoopPathUtils() {
    }

    public static List<Path> getPaths(List<String> files, Configuration conf, String fileSystemProperty) throws IOException {
        if (null == files || files.isEmpty()) {
            return Collections.emptyList();
        }

        List<Path> paths = new ArrayList<>();
        for (String file : files) {
            Path path = new Path(fileSystemProperty + file);
            FileSystem fileSystem = path.getFileSystem(conf);
            paths.addAll(getAllPaths(fileSystem, path));
        }
        return paths;
    }

    private static List<Path> getAllPaths(FileSystem fileSystem, Path path) throws IOException {
        List<Path> paths = new ArrayList<>();
        FileStatus[] statuses = fileSystem.listStatus(path);
        for (FileStatus status : statuses) {
            if (status.isDirectory()) {
                paths.addAll(getAllPaths(fileSystem, status.getPath()));
            } else {
                if (!status.getPath().getName().endsWith(".crc")) {
                    paths.add(status.getPath());
                }
            }
        }
        return paths;
    }
}

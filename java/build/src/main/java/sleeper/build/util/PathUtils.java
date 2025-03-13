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
package sleeper.build.util;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

public class PathUtils {

    private PathUtils() {
    }

    public static Path commonPath(Path path1, Path path2) {
        if (!Objects.equals(path1.getRoot(), path2.getRoot())) {
            throw new IllegalArgumentException("Paths have different roots: " + path1.getRoot() + ", " + path2.getRoot());
        }
        Path root = path1.getRoot();
        int names = Math.min(path1.getNameCount(), path2.getNameCount());
        for (int i = names - 1; i > 0; i--) {
            Path path = path1.subpath(0, i);
            if (path.equals(path2.subpath(0, i))) {
                if (root != null) {
                    return root.resolve(path);
                } else {
                    return path;
                }
            }
        }
        if (root == null) {
            return Paths.get("");
        }
        return root;
    }
}

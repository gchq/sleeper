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
package sleeper.sketchesv2.store;

import sleeper.core.schema.Schema;
import sleeper.sketchesv2.Sketches;
import sleeper.sketchesv2.SketchesSerDe;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class LocalFileSystemSketchesStore implements SketchesStore {

    @Override
    public void saveFileSketches(String filename, Schema schema, Sketches sketches) {
        Path path = readPath(filename);
        Path directory = path.getParent();
        try {
            if (directory != null) {
                Files.createDirectories(directory);
            }
            try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(path))) {
                new SketchesSerDe(schema).serialise(sketches, out);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Sketches loadFileSketches(String filename, Schema schema) {
        Path path = readPath(filename);
        try (DataInputStream in = new DataInputStream(Files.newInputStream(path))) {
            return new SketchesSerDe(schema).deserialise(in);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static Path readPath(String filename) {
        return Path.of(stripScheme(filename));
    }

    private static String stripScheme(String filename) {
        int schemePos = filename.indexOf("://");
        if (schemePos >= 0) {
            return filename.substring(schemePos + 3);
        } else {
            return filename;
        }
    }

}

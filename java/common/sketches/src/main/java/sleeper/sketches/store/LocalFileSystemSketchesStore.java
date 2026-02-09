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
package sleeper.sketches.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.schema.Schema;
import sleeper.sketches.Sketches;
import sleeper.sketches.SketchesSerDe;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class LocalFileSystemSketchesStore implements SketchesStore {
    public static final Logger LOGGER = LoggerFactory.getLogger(LocalFileSystemSketchesStore.class);

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
        LOGGER.info("Wrote sketches to local file {}", path);
    }

    @Override
    public Sketches loadFileSketches(String filename, Schema schema) {
        LOGGER.info("Loading sketches for file {}", filename);
        Path path = readPath(filename);
        try (DataInputStream in = new DataInputStream(Files.newInputStream(path))) {
            return new SketchesSerDe(schema).deserialise(in);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static Path readPath(String filename) {
        return Path.of(stripScheme(filename).replace(".parquet", ".sketches"));
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

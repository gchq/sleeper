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
package sleeper.bulkimport.runner.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.schema.Schema;
import sleeper.sketches.Sketches;
import sleeper.sketches.SketchesSerDe;
import sleeper.sketches.store.SketchesStore;

import java.io.IOException;
import java.io.UncheckedIOException;

public class HadoopSketchesStore implements SketchesStore {

    private final Configuration conf;

    public HadoopSketchesStore(Configuration conf) {
        this.conf = conf;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(HadoopSketchesStore.class);

    @Override
    public void saveFileSketches(String filename, Schema schema, Sketches sketches) {
        Path path = configPath(filename);
        try (FSDataOutputStream dataOutputStream = path.getFileSystem(conf).create(path)) {
            new SketchesSerDe(schema).serialise(sketches, dataOutputStream);
            LOGGER.info("Wrote sketches to {}", path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Sketches loadFileSketches(String filename, Schema schema) {
        LOGGER.info("Loading sketches for file {}", filename);
        try {
            Path path = configPath(filename);
            Sketches sketches;

            try (FSDataInputStream dataInputStream = path.getFileSystem(conf).open(path)) {
                sketches = new SketchesSerDe(schema).deserialise(dataInputStream);
            }
            return sketches;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Path configPath(String filename) {
        return new Path(filename.replace(".parquet", ".sketches"));
    }
}

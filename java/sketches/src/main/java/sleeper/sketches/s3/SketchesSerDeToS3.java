/*
 * Copyright 2022 Crown Copyright
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
package sleeper.sketches.s3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import sleeper.core.schema.Schema;
import sleeper.sketches.SketchSerialiser;
import sleeper.sketches.Sketches;

import java.io.IOException;

public class SketchesSerDeToS3 {
    private final Schema schema;

    public SketchesSerDeToS3(Schema schema) {
        this.schema = schema;
    }

    public void saveToHadoopFS(Path path, Sketches sketches, Configuration conf) throws IOException {
        FSDataOutputStream dataOutputStream = path.getFileSystem(conf).create(path);
        new SketchSerialiser(schema).serialise(sketches, dataOutputStream);
        dataOutputStream.close();
    }

    public void saveToHadoopFS(String fs, String file, Sketches sketches, Configuration conf) throws IOException {
        Path path = new Path(fs + file);
        saveToHadoopFS(path, sketches, conf);
    }

    public Sketches loadFromHadoopFS(Path path, Configuration conf) throws IOException {
        FSDataInputStream dataInputStream = path.getFileSystem(conf).open(path);
        Sketches sketches = new SketchSerialiser(schema).deserialise(dataInputStream);
        dataInputStream.close();
        return sketches;
    }

    public Sketches loadFromHadoopFS(String fs, String file, Configuration conf) throws IOException {
        Path path = new Path(fs + file);
        return loadFromHadoopFS(path, conf);
    }
}

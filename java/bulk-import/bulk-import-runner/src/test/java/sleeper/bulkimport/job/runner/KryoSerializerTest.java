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

package sleeper.bulkimport.job.runner;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.objenesis.strategy.StdInstantiatorStrategy;

import sleeper.bulkimport.job.runner.dataframelocalsort.JdkImmutableListRegistrator;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class KryoSerializerTest {
    @TempDir
    private Path tempDir;

    @Test
    void shouldSerializeAndDeserializePartition() throws IOException {
        // Given
        Kryo kryo = new Kryo();
        kryo.setRegistrationRequired(false);
        kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
        new JdkImmutableListRegistrator().registerClasses(kryo);
        kryo.register(Partition.class);
        Partition partition = new PartitionFactory(schemaWithKey("key"))
                .partition("test-partition", Long.MIN_VALUE, null);

        // When
        Output output = new Output(Files.newOutputStream(tempDir.resolve("test-file")));
        kryo.writeObject(output, partition);
        output.close();

        // Then
        Input input = new Input(Files.newInputStream(tempDir.resolve("test-file")));
        Partition result = kryo.readObject(input, Partition.class);
        assertThat(result).isEqualTo(partition);
        input.close();
    }
}

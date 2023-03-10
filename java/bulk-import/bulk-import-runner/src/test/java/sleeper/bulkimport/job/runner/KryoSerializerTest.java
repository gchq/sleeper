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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;

public class KryoSerializerTest {
    @TempDir
    private Path tempDir;

    @Test
    @Disabled("TODO")
    void shouldSerializeUnmodifiableList() throws IOException {
        Kryo kryo = new Kryo();
        kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
        kryo.register(DummySerializableClass.class);

        DummySerializableClass object = new DummySerializableClass("abc");

        Output output = new Output(Files.newOutputStream(tempDir.resolve("test-file")));
        kryo.writeObject(output, object);
        output.close();

        Input input = new Input(Files.newInputStream(tempDir.resolve("test-file")));
        assertThatCode(() -> kryo.readObject(input, DummySerializableClass.class))
                .doesNotThrowAnyException();
        input.close();
    }
}

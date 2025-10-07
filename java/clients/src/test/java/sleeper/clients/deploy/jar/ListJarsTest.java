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
package sleeper.clients.deploy.jar;

import org.junit.jupiter.api.Test;

import sleeper.core.deploy.ClientJar;
import sleeper.core.deploy.LambdaJar;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ListJarsTest {

    @Test
    void shouldOutputLambdaJar() {
        // Given
        LambdaJar jar = LambdaJar.builder()
                .filenameFormat("statestore-lambda-%s.jar")
                .imageName("statestore-lambda")
                .artifactId("statestore-lambda")
                .build();

        // When
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ListJars.print(List.of(jar), List.of(), "1.2.3", new PrintStream(out, true));

        // Then
        assertThat(out.toString()).isEqualTo("statestore-lambda:utility:statestore-lambda-1.2.3.jar\n");
    }

    @Test
    void shouldOutputClientJar() {
        // Given
        ClientJar jar = ClientJar.builder()
                .filenameFormat("clients-%s-utility.jar")
                .artifactId("clients")
                .build();

        // When
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ListJars.print(List.of(), List.of(jar), "1.2.3", new PrintStream(out, true));

        // Then
        assertThat(out.toString()).isEqualTo("clients:utility:clients-1.2.3-utility.jar\n");
    }

}

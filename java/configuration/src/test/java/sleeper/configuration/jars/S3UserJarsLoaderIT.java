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
package sleeper.configuration.jars;

import com.google.common.io.ByteStreams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.core.sync.RequestBody;

import sleeper.core.iterator.SortedRowIterator;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.ObjectFactory;
import sleeper.localstack.test.LocalStackTestBase;

import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.USER_JARS;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

class S3UserJarsLoaderIT extends LocalStackTestBase {

    private static final String SOURCE_CODE = "" +
            "import sleeper.core.row.Row;\n" +
            "import sleeper.core.schema.Schema;\n" +
            "import sleeper.core.iterator.CloseableIterator;\n" +
            "import sleeper.core.iterator.SortedRowIterator;\n" +
            "import java.util.List;\n" +
            "\n" +
            "public class MyIterator implements SortedRowIterator {\n" +
            "    public MyIterator() {}\n" +
            "\n" +
            "    @Override\n" +
            "    public void init(String configString, Schema schema) {}\n" +
            "\n" +
            "    @Override\n" +
            "    public CloseableIterator<Row> apply(CloseableIterator<Row> it) {return it;}\n" +
            "\n" +
            "    @Override\n" +
            "    public String toString() {return \"MyIterator\";}\n" +
            "\n" +
            "    @Override\n" +
            "    public List<String> getRequiredValueFields() { return null; }\n" +
            "}\n";
    private static final byte[] JAR_BYTES = compileClassCodeToJar("MyIterator", SOURCE_CODE);

    InstanceProperties instanceProperties = createTestInstanceProperties();
    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        createBucket(instanceProperties.get(JARS_BUCKET));
    }

    @Test
    public void shouldLoadCode() throws Exception {
        // Given
        writeJarToBucket("iterator.jar", JAR_BYTES);
        instanceProperties.set(USER_JARS, "iterator.jar");

        // When
        SortedRowIterator sri = objectFactory().getObject("MyIterator", SortedRowIterator.class);

        // Then
        assertThat(sri).hasToString("MyIterator");
    }

    @Test
    public void shouldLoadCodeTwice() throws Exception {
        // Given
        writeJarToBucket("iterator.jar", JAR_BYTES);
        instanceProperties.set(USER_JARS, "iterator.jar");
        objectFactory().getObject("MyIterator", SortedRowIterator.class);

        // When
        SortedRowIterator sri = objectFactory().getObject("MyIterator", SortedRowIterator.class);

        // Then
        assertThat(sri).hasToString("MyIterator");
    }

    private ObjectFactory objectFactory() throws Exception {
        return new S3UserJarsLoader(instanceProperties, s3Client, tempDir).buildObjectFactory();
    }

    private void writeJarToBucket(String objectKey, byte[] data) {
        s3Client.putObject(request -> request
                .bucket(instanceProperties.get(JARS_BUCKET))
                .key(objectKey),
                RequestBody.fromBytes(data));
    }

    private static byte[] compileClassCodeToJar(String className, String sourceCode) {
        // Compile class
        ToolProvider.getSystemJavaCompiler()
                .getTask(null, null, null,
                        List.of(), List.of(), List.of(new MySimpleJavaFileObject(className, sourceCode)))
                .call();
        String classFileName = className + ".class";
        try {
            // Read output class file and write to jar format in memory
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (JarOutputStream jos = new JarOutputStream(bytes, new Manifest())) {
                jos.putNextEntry(new JarEntry(classFileName));
                try (FileInputStream fis = new FileInputStream(classFileName)) {
                    ByteStreams.copy(fis, jos);
                }
            }
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            // Delete output class file
            try {
                Files.delete(Paths.get(classFileName));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    static class MySimpleJavaFileObject extends SimpleJavaFileObject {
        private final String code;

        MySimpleJavaFileObject(String name, String code) {
            super(create(name), Kind.SOURCE);
            this.code = code;
        }

        public String getCharContent(boolean ignoreEncodingErrors) {
            return code;
        }
    }

    private static URI create(String name) {
        String uri = "string:///" + name.replace(".", "/") + JavaFileObject.Kind.SOURCE.extension;
        return URI.create(uri);
    }
}

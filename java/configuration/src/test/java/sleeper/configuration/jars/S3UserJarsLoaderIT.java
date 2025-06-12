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

import sleeper.core.iterator.SortedRecordIterator;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.ObjectFactory;
import sleeper.localstack.test.LocalStackTestBase;

import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
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

    InstanceProperties instanceProperties = createTestInstanceProperties();
    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        createBucket(instanceProperties.get(JARS_BUCKET));
    }

    @Test
    public void shouldLoadCode() throws Exception {
        // Given a class implementing SortedRecordIterator
        String sourceCode = "" +
                "import sleeper.core.record.Record;\n" +
                "import sleeper.core.schema.Schema;\n" +
                "import sleeper.core.iterator.CloseableIterator;\n" +
                "import sleeper.core.iterator.SortedRecordIterator;\n" +
                "import java.util.List;\n" +
                "\n" +
                "public class MyIterator implements SortedRecordIterator {\n" +
                "    public MyIterator() {}\n" +
                "\n" +
                "    @Override\n" +
                "    public void init(String configString, Schema schema) {}\n" +
                "\n" +
                "    @Override\n" +
                "    public CloseableIterator<Record> apply(CloseableIterator<Record> it) {return it;}\n" +
                "\n" +
                "    @Override\n" +
                "    public String toString() {return \"MyIterator\";}\n" +
                "\n" +
                "    @Override\n" +
                "    public List<String> getRequiredValueFields() { return null; }\n" +
                "}\n";
        byte[] jarBytes = compileClassCodeToJar("MyIterator", sourceCode);
        writeJarToBucket("iterator.jar", jarBytes);
        instanceProperties.set(USER_JARS, "iterator.jar");

        // When
        SortedRecordIterator sri = objectFactory().getObject("MyIterator", SortedRecordIterator.class);

        // Then
        assertThat(sri).hasToString("MyIterator");
    }

    private ObjectFactory objectFactory() throws Exception {
        return new S3UserJarsLoader(instanceProperties, s3ClientV2, tempDir).buildObjectFactory();
    }

    private void writeJarToBucket(String objectKey, byte[] data) {
        s3ClientV2.putObject(request -> request
                .bucket(instanceProperties.get(JARS_BUCKET))
                .key(objectKey),
                RequestBody.fromBytes(data));
    }

    private byte[] compileClassCodeToJar(String className, String sourceCode) throws Exception {
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
        } finally {
            // Delete output class file
            Files.delete(Paths.get(classFileName));
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

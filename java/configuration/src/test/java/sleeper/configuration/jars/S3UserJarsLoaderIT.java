/*
 * Copyright 2022-2024 Crown Copyright
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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.google.common.io.ByteStreams;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.core.CommonTestConstants;
import sleeper.core.iterator.SortedRecordIterator;
import sleeper.core.properties.instance.InstanceProperties;

import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.UUID;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.USER_JARS;

@Testcontainers
public class S3UserJarsLoaderIT {
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE)).withServices(
            LocalStackContainer.Service.SQS, LocalStackContainer.Service.DYNAMODB, LocalStackContainer.Service.S3);

    @TempDir
    public Path folder;

    private AmazonS3 createS3Client() {
        return buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    }

    private InstanceProperties createInstanceProperties(AmazonS3 s3Client) {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, UUID.randomUUID().toString().substring(0, 18));
        instanceProperties.set(CONFIG_BUCKET, UUID.randomUUID().toString());
        instanceProperties.set(JARS_BUCKET, UUID.randomUUID().toString());
        instanceProperties.set(FILE_SYSTEM, "");
        s3Client.createBucket(instanceProperties.get(CONFIG_BUCKET));
        s3Client.createBucket(instanceProperties.get(JARS_BUCKET));
        return instanceProperties;
    }

    @Test
    public void shouldLoadCode() throws IOException, ObjectFactoryException {
        // Create a class implementing SortedRecordIterator
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
        MySimpleJavaFileObject fileObject = new MySimpleJavaFileObject("MyIterator", sourceCode);
        // Compile class and write to jar in temp directory
        ToolProvider.getSystemJavaCompiler()
                .getTask(null, null, null, Collections.emptyList(), Collections.emptyList(), Collections.singletonList(fileObject))
                .call();
        String jarFileLocation = createTempDirectory(folder, null).toString() + "/ajar.jar";
        JarOutputStream jos = new JarOutputStream(new FileOutputStream(jarFileLocation), new Manifest());
        JarEntry jarEntry = new JarEntry("MyIterator.class");
        jos.putNextEntry(jarEntry);
        FileInputStream fis = new FileInputStream("MyIterator.class");
        ByteStreams.copy(fis, jos);
        jos.close();
        // Upload jar to S3
        AmazonS3 s3Client = createS3Client();
        InstanceProperties instanceProperties = createInstanceProperties(s3Client);
        instanceProperties.set(USER_JARS, "iterator.jar");
        PutObjectRequest pubObjectRequest = new PutObjectRequest(instanceProperties.get(JARS_BUCKET), "iterator.jar", new File(jarFileLocation));
        s3Client.putObject(pubObjectRequest);
        // Delete local class file
        Files.delete(new File("MyIterator.class").toPath());
        // Create ObjectFactory and use to create iterator
        ObjectFactory objectFactory = new S3UserJarsLoader(instanceProperties, s3Client, createTempDirectory(folder, null).toString()).buildObjectFactory();
        SortedRecordIterator sri = objectFactory.getObject("MyIterator", SortedRecordIterator.class);

        assertThat(sri).hasToString("MyIterator");
    }

    public static class MySimpleJavaFileObject extends SimpleJavaFileObject {
        private final String code;

        public MySimpleJavaFileObject(String name, String code) {
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

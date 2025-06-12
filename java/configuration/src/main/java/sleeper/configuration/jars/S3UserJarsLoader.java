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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import sleeper.core.properties.instance.CommonProperty;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.ObjectFactory;
import sleeper.core.util.ObjectFactoryException;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Loads jars from the S3 jars bucket into the classpath dynamically. Only includes jars specified in the user jars
 * instance property, see {@link CommonProperty#USER_JARS}.
 */
public class S3UserJarsLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3UserJarsLoader.class);

    private final InstanceProperties instanceProperties;
    private final S3Client s3Client;
    private final Path localDir;

    public S3UserJarsLoader(InstanceProperties instanceProperties, S3Client s3Client, Path localDir) {
        this.instanceProperties = instanceProperties;
        this.s3Client = s3Client;
        this.localDir = localDir;
    }

    /**
     * Builds an object factory to load from either the current classpath, or the specified user jars.
     *
     * @return                        the object factory
     * @throws ObjectFactoryException if we could not build a URL to reference the local file a jar was downloaded to
     */
    public ObjectFactory buildObjectFactory() throws ObjectFactoryException {
        return new ObjectFactory(getClassLoader());
    }

    private ClassLoader getClassLoader() throws ObjectFactoryException {
        List<String> userJarsFiles = instanceProperties.getList(CommonProperty.USER_JARS);
        try {
            ClassLoader classLoader = getClassLoader(userJarsFiles);
            LOGGER.info("Created ClassLoader from jars {}", userJarsFiles);
            return classLoader;
        } catch (MalformedURLException e) {
            throw new ObjectFactoryException("MalformedURLException creating class loader from files " + userJarsFiles, e);
        }
    }

    private ClassLoader getClassLoader(List<String> jars) throws MalformedURLException {
        List<String> localJars = loadJars(jars);
        URL[] urls = new URL[localJars.size()];
        for (int i = 0; i < localJars.size(); i++) {
            urls[i] = (new File(localJars.get(i)).toURI().toURL());
        }
        return new URLClassLoader(urls, this.getClass().getClassLoader());
    }

    private List<String> loadJars(List<String> jarsList) {
        List<String> localJars = new ArrayList<>();
        for (String jar : jarsList) {
            localJars.add(loadJar(jar));
        }
        return localJars;
    }

    private String loadJar(String jar) {
        String bucket = instanceProperties.get(CommonProperty.JARS_BUCKET);
        Path outputFile = localDir.resolve(jar);
        if (Files.exists(outputFile)) {
            LOGGER.info("Found jar already exists locally, skipping: {}", outputFile);
            return outputFile.toString();
        }
        try {
            GetObjectResponse response = s3Client.getObject(request -> request
                    .bucket(bucket).key(jar),
                    ResponseTransformer.toFile(outputFile));
            outputFile.toFile().deleteOnExit();
            LOGGER.info("Loaded jar {} of size {} from {} and wrote to {}",
                    jar, response.contentLength(), bucket, outputFile);
        } catch (SdkClientException e) {
            if (isFileAlreadyExists(e)) {
                LOGGER.info("Found jar already exists locally after download attempt, skipping: {}", outputFile);
            } else {
                throw e;
            }
        }
        return outputFile.toString();
    }

    private static boolean isFileAlreadyExists(RuntimeException e) {
        Throwable check = e;
        do {
            if (check instanceof FileAlreadyExistsException) {
                return true;
            }
            check = check.getCause();
        } while (check != null);
        return false;
    }
}

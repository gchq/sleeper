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
package sleeper.configuration.jars;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.configuration.properties.InstanceProperties;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.JARS_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.USER_JARS;

public class ObjectFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(ObjectFactory.class);

    private final InstanceProperties instanceProperties;
    private final AmazonS3 s3Client;
    private final ClassLoader classLoader;

    public ObjectFactory(InstanceProperties instanceProperties, AmazonS3 s3Client, String localDir) throws ObjectFactoryException {
        this.instanceProperties = instanceProperties;
        this.s3Client = s3Client;
        List<String> userJarsFiles = instanceProperties.getList(USER_JARS);
        if (null != userJarsFiles) {
            try {
                this.classLoader = getClassLoader(userJarsFiles, localDir);
                LOGGER.info("Created ClassLoader from jars {}", userJarsFiles);
            } catch (MalformedURLException e) {
                throw new ObjectFactoryException("MalformedURLException creating class loader from files " + userJarsFiles, e);
            }
        } else {
            this.classLoader = this.getClass().getClassLoader();
        }
    }

    public <T> T getObject(String className, Class<T> parentClass) throws ObjectFactoryException {
        T object;
        try {
            object = classLoader
                    .loadClass(className)
                    .asSubclass(parentClass)
                    .getDeclaredConstructor()
                    .newInstance();
            LOGGER.info("Created object of class {} as subclass of {}", className, parentClass.getName());
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            throw new ObjectFactoryException("Exception instantiating object of class " + className
                    + " as subclass of " + parentClass.getName(), e);
        }
        return object;
    }

    private ClassLoader getClassLoader(List<String> jars, String localDir) throws MalformedURLException {
        List<String> localJars = loadJars(jars, localDir);
        URL[] urls = new URL[localJars.size()];
        for (int i = 0; i < localJars.size(); i++) {
            urls[i] = (new File(localJars.get(i)).toURI().toURL());
        }
        return new URLClassLoader(urls, this.getClass().getClassLoader());
    }

    private List<String> loadJars(List<String> jarsList, String localDir) {
        List<String> localJars = new ArrayList<>();
        for (String jar : jarsList) {
            localJars.add(loadJar(jar, localDir));
        }
        return localJars;
    }

    private String loadJar(String jar, String localDir) {
        GetObjectRequest getObjectRequest = new GetObjectRequest(instanceProperties.get(JARS_BUCKET), jar);
        String outputFile = localDir + "/" + jar;
        ObjectMetadata metadata = s3Client.getObject(getObjectRequest, new File(outputFile));
        LOGGER.info("Loaded jar {} of size {} from {} and wrote to {}",
                jar, metadata.getContentLength(), instanceProperties.get(JARS_BUCKET), outputFile);
        return outputFile;
    }
}

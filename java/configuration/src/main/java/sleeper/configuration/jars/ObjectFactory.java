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
package sleeper.configuration.jars;

import com.amazonaws.services.s3.AmazonS3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;

import java.lang.reflect.InvocationTargetException;

public class ObjectFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(ObjectFactory.class);

    private final ClassLoader classLoader;

    public ObjectFactory(InstanceProperties instanceProperties, AmazonS3 s3Client, String localDir) throws ObjectFactoryException {
        this.classLoader = new S3UserJarsLoader(instanceProperties, s3Client, localDir).getClassLoader();
    }

    private ObjectFactory() {
        classLoader = ObjectFactory.class.getClassLoader();
    }

    public static ObjectFactory noUserJars() {
        return new ObjectFactory();
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
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException |
                 InvocationTargetException e) {
            throw new ObjectFactoryException("Exception instantiating object of class " + className
                    + " as subclass of " + parentClass.getName(), e);
        }
        return object;
    }
}

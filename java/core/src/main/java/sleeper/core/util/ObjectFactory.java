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
package sleeper.core.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;

/**
 * Creates instances of classes that may be held in an external jar to be dynamically loaded into the classpath.
 */
public class ObjectFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(ObjectFactory.class);

    private final ClassLoader classLoader;

    public ObjectFactory(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    /**
     * Creates an instance of this class which will use the current classpath without any dynamic loading of jars.
     *
     * @return the instance
     */
    public static ObjectFactory noUserJars() {
        return new ObjectFactory(ObjectFactory.class.getClassLoader());
    }

    /**
     * Creates an instance of the given class.
     *
     * @param  <T>                    the type of the object to be created
     * @param  className              the binary name of the class, see {@link ClassLoader}
     * @param  parentClass            the type to cast the new object to
     * @return                        the new instance
     * @throws ObjectFactoryException if the object could not be instantiated
     */
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
}

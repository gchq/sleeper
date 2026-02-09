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
package sleeper.trino.utils;

public class SleeperSerDeUtils {

    private SleeperSerDeUtils() {
    }

    /**
     * Converts a string version of an object into an object of a specified type. It is a bit of a hack and it would be
     * good to remove the need for it.
     *
     * @param  objectTypeAsString  The full name of the type to generate, such as "java.lang.String".
     * @param  objectValueAsString The object value, as a String.
     * @return                     The parsed object.
     */
    public static Object convertStringToObjectOfNamedType(String objectTypeAsString, String objectValueAsString) {
        if (objectValueAsString == null) {
            return null;
        }
        switch (objectTypeAsString) {
            case "java.lang.String":
                return objectValueAsString;
            case "java.lang.Integer":
                return Integer.parseInt(objectValueAsString);
            case "java.lang.Long":
                return Long.parseLong(objectValueAsString);
            default:
                throw new UnsupportedOperationException("Object type " + objectTypeAsString + " is not handled");
        }
    }
}

package sleeper.trino.utils;

public class SleeperSerDeUtils {
    /**
     * Converts a string version of an object into an object of a specified type. It is a bit of a hack and it would be
     * good to remove the need for it.
     *
     * @param objectTypeAsString  The full name of the type to generate, such as "java.lang.String".
     * @param objectValueAsString The object value, as a String.
     * @return The parsed object.
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

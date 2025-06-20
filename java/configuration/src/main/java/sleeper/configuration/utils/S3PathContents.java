package sleeper.configuration.utils;

import java.util.List;

/**
 * Results of expanding a path in S3.
 *
 * @param location the location in S3
 * @param files    the files found
 */
public record S3PathContents(S3Path location, List<S3FileDetails> files) {

    /**
     * Throws an exception if no files were found.
     *
     * @throws S3FileNotFoundException if no files were found
     */
    public void throwIfEmpty() throws S3FileNotFoundException {
        if (files.isEmpty()) {
            throw new S3FileNotFoundException(location.bucket(), location.prefix());
        }
    }

    /**
     * Checks if no files were found.
     *
     * @return true if no files were found
     */
    public boolean isEmpty() {
        return files.isEmpty();
    }

    /**
     * Returns the path that was requested for expansion.
     *
     * @return the path
     */
    public String requestedPath() {
        return location.requestedPath();
    }
}
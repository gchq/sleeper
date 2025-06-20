package sleeper.configuration.utils;

/**
 * A location in S3 to look for files.
 *
 * @param requestedPath the path requested before parsing
 * @param bucket        the S3 bucket name
 * @param prefix        prefix for object keys
 */
public record S3Path(String requestedPath, String bucket, String prefix) {

    /**
     * Parses a path from a request in an ingest job, bulk import job or ingest batcher submission.
     *
     * @param  path the path
     * @return      the parsed location in S3
     */
    public static S3Path parse(String path) {
        if (!path.contains("/")) {
            return new S3Path(path, path, "");
        } else {
            return new S3Path(path,
                    path.substring(0, path.indexOf("/")),
                    path.substring(path.indexOf("/") + 1));
        }
    }
}
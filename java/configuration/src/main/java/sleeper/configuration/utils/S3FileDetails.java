package sleeper.configuration.utils;

/**
 * A file found in an S3 bucket.
 *
 * @param bucket   the S3 bucket name
 * @param s3Object details of the S3 object
 */
public record S3FileDetails(String bucket, String objectKey, long fileSizeBytes) {

    /**
     * Builds a path to this file to be used in a job definition. This includes ingest jobs, bulk import jobs, and
     * submissions to the ingest batcher.
     *
     * @return the path
     */
    public String pathForJob() {
        return bucket() + "/" + objectKey();
    }

    /**
     * Builds a path to this file to be used by Hadoop.
     *
     * @return the path
     */
    public String pathForHadoop() {
        return "s3a://" + bucket() + "/" + objectKey();
    }
}
package sleeper.configuration.properties;

import sleeper.configuration.Utils;

import java.util.List;

public interface AsyncIngestPartitionFileWriterProperties {
    UserDefinedInstanceProperty ASYNC_INGEST_CLIENT_TYPE = Index.propertyBuilder("sleeper.ingest.async.client.type")
            .description("The implementation of the async S3 client to use for upload during ingest.\n" +
                    "Valid values are 'java' or 'crt'. This determines the implementation of S3AsyncClient that gets used.\n" +
                    "With 'java' it makes a single PutObject request for each file.\n" +
                    "With 'crt' it uses the AWS Common Runtime (CRT) to make multipart uploads.\n" +
                    "Note that the CRT option is recommended. Using the Java option may cause failures if any file is >5GB in size, and " +
                    "will lead to the following warning:\n" +
                    "\"The provided S3AsyncClient is not an instance of S3CrtAsyncClient, and thus multipart upload/download feature is not " +
                    "enabled and resumable file upload is not supported. To benefit from maximum throughput, consider using " +
                    "S3AsyncClient.crtBuilder().build() instead.\"\n" +
                    "(async partition file writer only)")
            .defaultValue("crt")
            .propertyGroup(InstancePropertyGroup.INGEST).build();
    UserDefinedInstanceProperty ASYNC_INGEST_CRT_PART_SIZE_BYTES = Index.propertyBuilder("sleeper.ingest.async.crt.part.size.bytes")
            .description("The part size in bytes to use for multipart uploads.\n" +
                    "(CRT async ingest only) [128MB]")
            .defaultValue("134217728") // 128M
            .validationPredicate(Utils::isPositiveLong)
            .propertyGroup(InstancePropertyGroup.INGEST).build();
    UserDefinedInstanceProperty ASYNC_INGEST_CRT_TARGET_THROUGHPUT_GBPS = Index.propertyBuilder("sleeper.ingest.async.crt.target.throughput.gbps")
            .description("The target throughput for multipart uploads, in GB/s. Determines how many parts should be uploaded simultaneously.\n" +
                    "(CRT async ingest only)")
            .defaultValue("10")
            .validationPredicate(Utils::isPositiveDouble)
            .propertyGroup(InstancePropertyGroup.INGEST).build();

    static List<UserDefinedInstanceProperty> getAll() {
        return Index.INSTANCE.getAll();
    }

    static boolean has(String propertyName) {
        return Index.INSTANCE.getByName(propertyName).isPresent();
    }

    class Index {
        private Index() {
        }

        private static final SleeperPropertyIndex<UserDefinedInstanceProperty> INSTANCE = new SleeperPropertyIndex<>();





        static UserDefinedInstancePropertyImpl.Builder propertyBuilder(String propertyName) {
            return UserDefinedInstancePropertyImpl.named(propertyName)
                    .addToIndex(INSTANCE::add);
        }
    }
}

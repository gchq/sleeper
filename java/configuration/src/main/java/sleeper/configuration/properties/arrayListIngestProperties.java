package sleeper.configuration.properties;

import java.util.List;

public interface arrayListIngestProperties {
    UserDefinedInstanceProperty MAX_RECORDS_TO_WRITE_LOCALLY = Index.propertyBuilder("sleeper.ingest.max.local.records")
            .description("The maximum number of records written to local file in an ingest job. (Records are written in sorted order to local " +
                    "disk before being uploaded to S3. Increasing this value increases the amount of time before data is visible in the " +
                    "system, but increases the number of records written to S3 in a batch, therefore reducing costs.)\n" +
                    "(arraylist-based ingest only)")
            .defaultValue("100000000")
            .propertyGroup(InstancePropertyGroup.INGEST).build();
    UserDefinedInstanceProperty MAX_IN_MEMORY_BATCH_SIZE = Index.propertyBuilder("sleeper.ingest.memory.max.batch.size")
            .description("The maximum number of records to read into memory in an ingest job. (Up to sleeper.ingest.memory.max.batch.size " +
                    "records are read into memory before being sorted and written to disk. This process is repeated until " +
                    "sleeper.ingest.max.local.records records have been written to local files. Then the sorted files and merged and " +
                    "the data is written to sorted files in S3.)\n" +
                    "(arraylist-based ingest only)")
            .defaultValue("1000000")
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

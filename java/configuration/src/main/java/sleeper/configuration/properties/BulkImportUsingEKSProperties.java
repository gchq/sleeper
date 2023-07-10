package sleeper.configuration.properties;

import java.util.List;

public interface BulkImportUsingEKSProperties {
    UserDefinedInstanceProperty BULK_IMPORT_REPO = Index.propertyBuilder("sleeper.bulk.import.eks.repo")
            .description("(EKS mode only) The name of the ECS repository where the Docker image for the bulk import container is stored.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();

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

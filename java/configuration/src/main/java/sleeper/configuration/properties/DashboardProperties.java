package sleeper.configuration.properties;

import sleeper.configuration.Utils;

import java.util.List;

public interface DashboardProperties {
    UserDefinedInstanceProperty DASHBOARD_TIME_WINDOW_MINUTES = Index.propertyBuilder("sleeper.dashboard.time.window.minutes")
            .description("The period in minutes used in the dashboard.")
            .defaultValue("5")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.DASHBOARD)
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

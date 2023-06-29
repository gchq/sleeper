package sleeper.ingest.job.status;

import sleeper.core.record.process.status.ProcessFinishedStatus;
import sleeper.core.record.process.status.ProcessStatusUpdate;

import java.util.stream.Stream;

public enum IngestJobStatusType {
    REJECTED(IngestJobRejectedStatus.class, 1),
    ACCEPTED(IngestJobAcceptedStatus.class, 2),
    STARTED(IngestJobStartedStatus.class, 3),
    FINISHED(ProcessFinishedStatus.class, 4);

    private final Class<?> statusUpdateClass;
    private final int order;

    IngestJobStatusType(Class<?> statusUpdateClass, int order) {
        this.statusUpdateClass = statusUpdateClass;
        this.order = order;
    }

    public int getOrder() {
        return order;
    }

    public static IngestJobStatusType of(ProcessStatusUpdate update) {
        return Stream.of(values())
                .filter(type -> type.statusUpdateClass.isInstance(update))
                .findFirst().orElseThrow();
    }

}

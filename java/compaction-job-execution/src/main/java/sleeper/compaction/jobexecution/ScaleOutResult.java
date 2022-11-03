package sleeper.compaction.jobexecution;

/**
 * Result of a scaling out check.
 */
public enum ScaleOutResult {
    /** No scale up required. Cluster has spare container capacity. */
    NOT_REQUIRED,
    /** Scale up required, but already at maximum instances. */
    NO_SPARE_HEADROOM,
    /** Scaling operation already in progress. Can't scale. */
    SCALING_IN_PROGRESS,
    /** Scaling operation has begun. */
    SCALING_INITIATED,
}

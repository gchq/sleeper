/*
 * Copyright 2022-2026 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sleeper.api.resources;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.table.TableStatus;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.core.tracker.ingest.job.query.IngestJobRun;
import sleeper.core.tracker.ingest.job.query.IngestJobStatus;
import sleeper.core.tracker.ingest.job.query.IngestJobStatusType;
import sleeper.core.tracker.ingest.task.IngestTaskFinishedStatus;
import sleeper.core.tracker.ingest.task.IngestTaskStatus;
import sleeper.core.tracker.ingest.task.IngestTaskTracker;
import sleeper.core.tracker.job.run.JobRunSummary;
import sleeper.ingest.tracker.job.IngestJobTrackerFactory;
import sleeper.ingest.tracker.task.IngestTaskTrackerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static sleeper.core.properties.instance.IngestProperty.INGEST_JOB_STATUS_TTL_IN_SECONDS;
import static sleeper.core.properties.instance.IngestProperty.INGEST_TRACKER_ENABLED;

@Path("/api/ingest-tracking")
public class IngestTrackingResource {

    private static final int DEFAULT_LIMIT = 100;
    private static final int MAX_LIMIT = 1000;

    private final S3Client s3Client;
    private final DynamoDbClient dynamoDbClient;
    private final String instanceId;
    private final String accountName;

    @Inject
    public IngestTrackingResource(
            S3Client s3Client,
            DynamoDbClient dynamoDbClient,
            @ConfigProperty(name = "sleeper.instance.id") String instanceId,
            @ConfigProperty(name = "sleeper.account.name") String accountName) {
        this.s3Client = s3Client;
        this.dynamoDbClient = dynamoDbClient;
        this.instanceId = instanceId;
        this.accountName = accountName;
    }

    @GET
    @Path("/jobs")
    @Produces(MediaType.APPLICATION_JSON)
    public IngestJobsResponse getJobs(
            @QueryParam("tableId") String tableId,
            @QueryParam("limit") Integer limitParam,
            @QueryParam("jobId") String jobIdFilter,
            @QueryParam("state") String stateFilter,
            @QueryParam("from") Long fromEpochMillis,
            @QueryParam("to") Long toEpochMillis) {
        InstanceProperties instanceProperties = loadPropertiesAndCheckEnabled();
        IngestJobTracker jobTracker = IngestJobTrackerFactory.getTracker(dynamoDbClient, instanceProperties);
        Map<String, String> tableNamesById = tableNamesById(instanceProperties);

        int limit = limitParam == null ? DEFAULT_LIMIT : Math.min(Math.max(1, limitParam), MAX_LIMIT);

        Instant from = fromEpochMillis == null ? Instant.MIN : Instant.ofEpochMilli(fromEpochMillis);
        Instant to = toEpochMillis == null ? Instant.MAX : Instant.ofEpochMilli(toEpochMillis);
        boolean hasTimeFilter = fromEpochMillis != null || toEpochMillis != null;
        StateFilter state = StateFilter.parse(stateFilter);

        List<IngestJobStatus> jobs = new ArrayList<>();
        if (jobIdFilter != null && !jobIdFilter.isBlank()) {
            jobTracker.getJob(jobIdFilter.trim()).ifPresent(jobs::add);
        } else {
            // The job tracker is partitioned by table id, so query each table
            Set<String> tableIds = tableId != null && !tableId.isBlank()
                    ? Set.of(tableId)
                    : tableNamesById.keySet();

            for (String id : tableIds) {
                jobs.addAll(jobTracker.getAllJobs(id));
            }
        }

        List<IngestJobSummary> summaries = new ArrayList<>();
        for (IngestJobStatus job : jobs) {
            if (!state.matches(job)) {
                continue;
            }
            if (hasTimeFilter && !job.isInPeriod(from, to)) {
                continue;
            }
            summaries.add(toSummary(job, tableNamesById));
        }

        // Newest first, by the start time of the latest run (jobs with no start time sort last).
        summaries.sort(Comparator.comparing(
                (IngestJobSummary summary) -> summary.startTime() == null ? "" : summary.startTime())
                .reversed());

        int total = summaries.size();
        int numToReturn = Math.min(limit, total);
        long jobStatusTtlSeconds = instanceProperties.getLong(INGEST_JOB_STATUS_TTL_IN_SECONDS);
        return new IngestJobsResponse(summaries.subList(0, numToReturn), limit, numToReturn < total, jobStatusTtlSeconds);
    }

    private enum StateFilter {
        ALL {
            boolean matches(IngestJobStatus job) {
                return true;
            }
        },
        REJECTED {
            boolean matches(IngestJobStatus job) {
                return job.getFurthestRunStatusType() == IngestJobStatusType.REJECTED;
            }
        },
        FAILED {
            boolean matches(IngestJobStatus job) {
                return job.getFurthestRunStatusType() == IngestJobStatusType.FAILED;
            }
        },
        RUNNING {
            boolean matches(IngestJobStatus job) {
                return job.isUnfinishedOrAnyRunInProgress()
                        && job.getFurthestRunStatusType() != IngestJobStatusType.REJECTED;
            }
        },
        FINISHED {
            boolean matches(IngestJobStatus job) {
                return job.isAnyRunSuccessful();
            }
        };

        abstract boolean matches(IngestJobStatus job);

        static StateFilter parse(String value) {
            if (value == null || value.isBlank()) {
                return ALL;
            }
            try {
                return StateFilter.valueOf(value.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                return ALL;
            }
        }
    }

    @GET
    @Path("/job/{jobId}")
    @Produces(MediaType.APPLICATION_JSON)
    public IngestJobDetail getJob(@PathParam("jobId") String jobId) {
        InstanceProperties instanceProperties = loadPropertiesAndCheckEnabled();
        DynamoDBTableIndex tableIndex = new DynamoDBTableIndex(instanceProperties, dynamoDbClient);
        IngestJobTracker jobTracker = IngestJobTrackerFactory.getTracker(dynamoDbClient, instanceProperties);
        IngestTaskTracker taskTracker = IngestTaskTrackerFactory.getTracker(dynamoDbClient, instanceProperties);

        IngestJobStatus job = jobTracker.getJob(jobId)
                .orElseThrow(() -> new WebApplicationException(
                        Response.status(Response.Status.NOT_FOUND)
                                .entity(new NotAvailable("job_not_found",
                                        "No ingest job found with id " + jobId + "."))
                                .type(MediaType.APPLICATION_JSON)
                                .build()));

        Map<String, IngestTaskView> tasksById = new LinkedHashMap<>();
        List<IngestJobRunView> runs = new ArrayList<>();
        for (IngestJobRun run : job.getRunsLatestFirst()) {
            String taskId = run.getTaskId();
            if (taskId != null && !tasksById.containsKey(taskId)) {
                IngestTaskStatus task = taskTracker.getTask(taskId);
                if (task != null) {
                    tasksById.put(taskId, toTaskView(task));
                }
            }
            runs.add(toRunView(run));
        }

        String tableName = job.getTableId() == null ? null : tableIndex.getTableByUniqueId(job.getTableId()).map(TableStatus::getTableName).orElse(null);

        return new IngestJobDetail(
                job.getJobId(),
                job.getTableId(),
                tableName,
                job.getInputFileCount(),
                toStringOrNull(job.getExpiryDate()),
                job.getFurthestRunStatusType().name(),
                runs,
                tasksById);
    }

    private InstanceProperties loadPropertiesAndCheckEnabled() {
        InstanceProperties instanceProperties = S3InstanceProperties.loadGivenAccountAndInstanceId(s3Client, accountName, instanceId);
        if (!instanceProperties.getBoolean(INGEST_TRACKER_ENABLED)) {
            throw new WebApplicationException(
                    Response.status(Response.Status.NOT_FOUND)
                            .entity(new NotAvailable("ingest_tracking_not_enabled",
                                    "Ingest tracking is not enabled for this instance."))
                            .type(MediaType.APPLICATION_JSON)
                            .build());
        }
        return instanceProperties;
    }

    private Map<String, String> tableNamesById(InstanceProperties instanceProperties) {
        DynamoDBTableIndex tableIndex = new DynamoDBTableIndex(instanceProperties, dynamoDbClient);
        return tableIndex.streamAllTables().collect(Collectors.toMap(
                TableStatus::getTableUniqueId, TableStatus::getTableName, (a, b) -> a, LinkedHashMap::new));
    }

    private static IngestJobSummary toSummary(IngestJobStatus job, Map<String, String> tableNamesById) {
        List<IngestJobRun> runs = job.getRunsLatestFirst();
        IngestJobRun latest = runs.stream().findFirst().orElse(null);
        Long rowsWritten = null;
        if (latest != null && latest.getFinishedSummary() != null) {
            rowsWritten = latest.getFinishedSummary().getRowsWritten();
        }
        return new IngestJobSummary(
                job.getJobId(),
                job.getTableId(),
                job.getTableId() == null ? null : tableNamesById.get(job.getTableId()),
                job.getInputFileCount(),
                job.getFurthestRunStatusType().name(),
                runs.size(),
                latest == null ? null : toStringOrNull(latest.getStartTime()),
                latest == null ? null : toStringOrNull(latest.getFinishTime()),
                rowsWritten);
    }

    private static IngestJobRunView toRunView(IngestJobRun run) {
        JobRunSummary summary = run.getFinishedSummary();
        return new IngestJobRunView(
                run.getTaskId(),
                run.getStatusType().name(),
                toStringOrNull(run.getStartTime()),
                toStringOrNull(run.getFinishTime()),
                run.isFinished(),
                run.isFinishedSuccessfully(),
                summary == null ? null : summary.getRowsRead(),
                summary == null ? null : summary.getRowsWritten(),
                summary == null ? null : summary.getDurationInSeconds(),
                run.getFailureReasons());
    }

    private static IngestTaskView toTaskView(IngestTaskStatus task) {
        IngestTaskFinishedStatus finished = task.getFinishedStatus();
        return new IngestTaskView(
                task.getTaskId(),
                toStringOrNull(task.getStartTime()),
                toStringOrNull(task.getFinishTime()),
                task.getDuration() == null ? null : task.getDuration().toMillis() / 1000.0,
                task.isFinished(),
                finished == null ? null : finished.getTotalRowsRead(),
                finished == null ? null : finished.getTotalRowsWritten(),
                finished == null ? null : finished.getTimeSpentOnJobs().toMillis() / 1000.0);
    }

    private static String toStringOrNull(Instant instant) {
        return instant == null ? null : instant.toString();
    }

    public record IngestJobSummary(
            String jobId, String tableId, String tableName, int inputFileCount, String status,
            int runCount, String startTime, String finishTime, Long rowsWritten) {
    }

    public record IngestJobsResponse(List<IngestJobSummary> jobs, int limit, boolean hasMore, long jobStatusTtlSeconds) {
    }

    public record IngestJobDetail(
            String jobId, String tableId, String tableName, int inputFileCount, String expiryDate,
            String status, List<IngestJobRunView> runs, Map<String, IngestTaskView> tasks) {
    }

    public record IngestJobRunView(
            String taskId, String status, String startTime, String finishTime, boolean finished,
            boolean finishedSuccessfully, Long rowsRead, Long rowsWritten, Double durationSeconds,
            List<String> failureReasons) {
    }

    public record IngestTaskView(
            String taskId, String startTime, String finishTime, Double durationSeconds, boolean finished,
            Long totalRowsRead, Long totalRowsWritten, Double timeSpentOnJobsSeconds) {
    }

    public record NotAvailable(String error, String message) {
    }

}

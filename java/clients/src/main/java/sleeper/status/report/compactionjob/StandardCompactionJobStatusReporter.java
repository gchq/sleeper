/*
 * Copyright 2022 Crown Copyright
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

package sleeper.status.report.compactionjob;

import sleeper.compaction.job.status.CompactionJobRun;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.core.record.process.AverageRecordRate;
import sleeper.status.report.table.TableField;
import sleeper.status.report.table.TableRow;
import sleeper.status.report.table.TableWriter;
import sleeper.status.report.table.TableWriterFactory;

import java.io.PrintStream;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static sleeper.ClientUtils.countWithCommas;
import static sleeper.ClientUtils.decimalWithCommas;

public class StandardCompactionJobStatusReporter implements CompactionJobStatusReporter {

    private static final TableWriterFactory.Builder TABLE_FACTORY_BUILDER = TableWriterFactory.builder();

    private static final TableField STATE = TABLE_FACTORY_BUILDER.addField("STATE");
    private static final TableField CREATE_TIME = TABLE_FACTORY_BUILDER.addField("CREATE_TIME");
    private static final TableField JOB_ID = TABLE_FACTORY_BUILDER.addField("JOB_ID");
    private static final TableField PARTITION_ID = TABLE_FACTORY_BUILDER.addField("PARTITION_ID");
    private static final TableField TYPE = TABLE_FACTORY_BUILDER.addField("TYPE");
    private static final TableField TASK_ID = TABLE_FACTORY_BUILDER.addField("TASK_ID");
    private static final TableField START_TIME = TABLE_FACTORY_BUILDER.addField("START_TIME");
    private static final TableField FINISH_TIME = TABLE_FACTORY_BUILDER.addField("FINISH_TIME");
    private static final TableField DURATION = TABLE_FACTORY_BUILDER.fieldBuilder("DURATION (s)").alignRight().build();
    private static final TableField LINES_READ = TABLE_FACTORY_BUILDER.fieldBuilder("LINES_READ").alignRight().build();
    private static final TableField LINES_WRITTEN = TABLE_FACTORY_BUILDER.fieldBuilder("LINES_WRITTEN").alignRight().build();
    private static final TableField READ_RATE = TABLE_FACTORY_BUILDER.fieldBuilder("READ_RATE (s)").alignRight().build();
    private static final TableField WRITE_RATE = TABLE_FACTORY_BUILDER.fieldBuilder("WRITE_RATE (s)").alignRight().build();

    private static final TableWriterFactory TABLE_FACTORY = TABLE_FACTORY_BUILDER.build();

    private static final String STATE_PENDING = "PENDING";
    private static final String STATE_IN_PROGRESS = "IN PROGRESS";
    private static final String STATE_FINISHED = "FINISHED";

    private final PrintStream out;

    public StandardCompactionJobStatusReporter() {
        this.out = System.out;
    }

    public StandardCompactionJobStatusReporter(PrintStream out) {
        this.out = out;
    }

    public void report(List<CompactionJobStatus> jobStatusList, QueryType queryType) {
        out.println();
        out.println("Compaction Job Status Report");
        out.println("----------------------------");
        printSummary(jobStatusList, queryType);
        if (!queryType.equals(QueryType.DETAILED)) {
            TABLE_FACTORY.tableBuilder()
                    .showFields(queryType != QueryType.UNFINISHED,
                            FINISH_TIME, DURATION, LINES_READ, LINES_WRITTEN, READ_RATE, WRITE_RATE)
                    .itemsAndSplittingWriter(jobStatusList, this::writeJob)
                    .build().write(out);
        }
    }

    private void printSummary(List<CompactionJobStatus> jobStatusList, QueryType queryType) {
        if (queryType.equals(QueryType.RANGE)) {
            printRangeSummary(jobStatusList);
        }
        if (queryType.equals(QueryType.DETAILED)) {
            printDetailedSummary(jobStatusList);
        }
        if (queryType.equals(QueryType.UNFINISHED)) {
            printUnfinishedSummary(jobStatusList);
        }
        if (queryType.equals(QueryType.ALL)) {
            printAllSummary(jobStatusList);
        }
    }

    private void printRangeSummary(List<CompactionJobStatus> jobStatusList) {
        out.printf("Total jobs in defined range: %d%n",
                jobStatusList.size());
        printAverageCompactionRate("Average compaction rate: %s%n", jobStatusList);
        printAverageCompactionRate("Average standard compaction rate: %s%n", standardJobs(jobStatusList));
        printAverageCompactionRate("Average splitting compaction rate: %s%n", splittingJobs(jobStatusList));
    }

    private void printDetailedSummary(List<CompactionJobStatus> jobStatusList) {
        if (jobStatusList.isEmpty()) {
            out.printf("No job found with provided jobId%n");
            out.printf("--------------------------%n");
        } else {
            jobStatusList.forEach(this::printSingleJobSummary);
        }
    }

    private void printSingleJobSummary(CompactionJobStatus jobStatus) {
        out.printf("Details for job %s:%n", jobStatus.getJobId());
        out.printf("State: %s%n", getState(jobStatus));
        out.printf("Creation Time: %s%n", jobStatus.getCreateUpdateTime().toString());
        out.printf("Partition ID: %s%n", jobStatus.getPartitionId());
        out.printf("Child partition IDs: %s%n", jobStatus.getChildPartitionIds().toString());
        for (CompactionJobRun run : jobStatus.getJobRuns()) {
            out.println();
            out.printf("Run on task %s%n", run.getTaskId());
            out.printf("Start Time: %s%n", run.getStartTime());
            out.printf("Start Update Time: %s%n", run.getStartUpdateTime());
            if (run.isFinished()) {
                out.printf("Finish Time: %s%n", run.getFinishTime());
                out.printf("Finish Update Time: %s%n", run.getFinishUpdateTime());
                out.printf("Duration: %ss%n", getDurationInSeconds(run));
                out.printf("Lines Read: %s%n", getLinesRead(run));
                out.printf("Lines Written: %s%n", getLinesWritten(run));
                out.printf("Read Rate (reads per second): %s%n", getRecordsReadPerSecond(run));
                out.printf("Write Rate (writes per second): %s%n", getRecordsWrittenPerSecond(run));
            } else {
                out.println("Not finished");
            }
        }
        out.println("--------------------------");
    }

    private void printUnfinishedSummary(List<CompactionJobStatus> jobStatusList) {
        out.printf("Total unfinished jobs: %d%n", jobStatusList.size());
        out.printf("Total unfinished jobs in progress: %d%n",
                jobStatusList.stream().filter(CompactionJobStatus::isStarted).count());
        out.printf("Total unfinished jobs not started: %d%n",
                jobStatusList.size() - jobStatusList.stream().filter(CompactionJobStatus::isStarted).count());
    }

    private void printAllSummary(List<CompactionJobStatus> jobStatusList) {
        List<CompactionJobStatus> splittingJobs = splittingJobs(jobStatusList);
        List<CompactionJobStatus> standardJobs = standardJobs(jobStatusList);
        out.printf("Total jobs: %d%n", jobStatusList.size());
        printAverageCompactionRate("Average compaction rate: %s%n", jobStatusList);
        out.println();
        out.printf("Total standard jobs: %d%n", standardJobs.size());
        out.printf("Total standard jobs pending: %d%n", standardJobs.stream().filter(job -> !job.isStarted()).count());
        out.printf("Total standard jobs in progress: %d%n", standardJobs.stream().filter(job -> job.isStarted() && !job.isFinished()).count());
        out.printf("Total standard jobs finished: %d%n", standardJobs.stream().filter(CompactionJobStatus::isFinished).count());
        printAverageCompactionRate("Average standard compaction rate: %s%n", standardJobs);
        out.println();
        out.printf("Total splitting jobs: %d%n", splittingJobs.size());
        out.printf("Total splitting jobs pending: %d%n", splittingJobs.stream().filter(job -> !job.isStarted()).count());
        out.printf("Total splitting jobs in progress: %d%n", splittingJobs.stream().filter(job -> job.isStarted() && !job.isFinished()).count());
        out.printf("Total splitting jobs finished: %d%n", splittingJobs.stream().filter(CompactionJobStatus::isFinished).count());
        printAverageCompactionRate("Average splitting compaction rate: %s%n", splittingJobs);
    }

    private static List<CompactionJobStatus> standardJobs(List<CompactionJobStatus> jobStatusList) {
        return jobStatusList.stream()
                .filter(job -> !job.isSplittingCompaction())
                .collect(Collectors.toList());
    }

    private static List<CompactionJobStatus> splittingJobs(List<CompactionJobStatus> jobStatusList) {
        return jobStatusList.stream()
                .filter(CompactionJobStatus::isSplittingCompaction)
                .collect(Collectors.toList());
    }

    private void printAverageCompactionRate(String formatString, List<CompactionJobStatus> jobs) {
        AverageRecordRate average = recordRate(jobs);
        if (average.getJobCount() < 1) {
            return;
        }
        String rateString = String.format("%s read/s, %s write/s",
                formatDecimal(average.getRecordsReadPerSecond()),
                formatDecimal(average.getRecordsWrittenPerSecond()));
        out.printf(formatString, rateString);
    }

    private static AverageRecordRate recordRate(List<CompactionJobStatus> jobs) {
        return AverageRecordRate.of(jobs.stream()
                .flatMap(job -> job.getJobRuns().stream())
                .filter(CompactionJobRun::isFinished)
                .map(CompactionJobRun::getFinishedSummary));
    }

    private void writeJob(CompactionJobStatus job, TableWriter.Builder table) {
        if (job.getJobRuns().isEmpty()) {
            table.row(row -> {
                row.value(STATE, STATE_PENDING);
                writeJobFields(job, row);
            });
        } else {
            job.getJobRuns().forEach(run -> table.row(row -> {
                writeJobFields(job, row);
                writeRunFields(run, row);
            }));
        }
    }

    private void writeJobFields(CompactionJobStatus job, TableRow.Builder builder) {
        builder.value(CREATE_TIME, job.getCreateUpdateTime())
                .value(JOB_ID, job.getJobId())
                .value(PARTITION_ID, job.getPartitionId())
                .value(TYPE, job.isSplittingCompaction() ? "SPLIT" : "COMPACT");
    }

    private void writeRunFields(CompactionJobRun run, TableRow.Builder builder) {
        builder.value(STATE, getState(run))
                .value(TASK_ID, run.getTaskId())
                .value(START_TIME, run.getStartTime())
                .value(FINISH_TIME, run.getFinishTime())
                .value(DURATION, getDurationInSeconds(run))
                .value(LINES_READ, getLinesRead(run))
                .value(LINES_WRITTEN, getLinesWritten(run))
                .value(READ_RATE, getRecordsReadPerSecond(run))
                .value(WRITE_RATE, getRecordsWrittenPerSecond(run));
    }

    private static String getState(CompactionJobRun run) {
        if (run.isFinished()) {
            return STATE_FINISHED;
        }
        return STATE_IN_PROGRESS;
    }

    private static String getState(CompactionJobStatus job) {
        if (job.isFinished()) {
            return STATE_FINISHED;
        } else if (job.isStarted()) {
            return STATE_IN_PROGRESS;
        }
        return STATE_PENDING;
    }

    private static String getDurationInSeconds(CompactionJobRun run) {
        return getOrNull(run.getFinishedSummary(), summary -> formatDecimal(summary.getDurationInSeconds()));
    }

    private static String getLinesRead(CompactionJobRun run) {
        return getOrNull(run.getFinishedSummary(), summary -> countWithCommas(summary.getLinesRead()));
    }

    private static String getLinesWritten(CompactionJobRun run) {
        return getOrNull(run.getFinishedSummary(), summary -> countWithCommas(summary.getLinesWritten()));
    }

    private static String getRecordsReadPerSecond(CompactionJobRun run) {
        return getOrNull(run.getFinishedSummary(), summary -> formatDecimal(summary.getRecordsReadPerSecond()));
    }

    private static String getRecordsWrittenPerSecond(CompactionJobRun run) {
        return getOrNull(run.getFinishedSummary(), summary -> formatDecimal(summary.getRecordsWrittenPerSecond()));
    }

    private static String formatDecimal(double value) {
        return decimalWithCommas("%.2f", value);
    }

    private static <I, O> O getOrNull(I object, Function<I, O> getter) {
        if (object == null) {
            return null;
        }
        return getter.apply(object);
    }
}

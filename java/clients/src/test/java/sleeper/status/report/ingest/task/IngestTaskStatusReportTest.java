package sleeper.status.report.ingest.task;

import org.junit.Test;
import sleeper.ToStringPrintStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ClientTestUtils.example;

public class IngestTaskStatusReportTest {

    @Test
    public void shouldReportNoIngestTasks() throws Exception {

        // When / Then
        assertThat(getStandardReport(IngestTaskQuery.ALL)).hasToString(
                example("reports/ingest/task/noTasksQueryingForAll.txt"));
    }

    private String getStandardReport(IngestTaskQuery query) {
        ToStringPrintStream output = new ToStringPrintStream();
        new IngestTaskStatusReport(output.getPrintStream()).run(query);
        return output.toString();
    }
}

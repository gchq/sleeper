package sleeper.status.report.ingest.task;

import sleeper.status.report.table.TableField;
import sleeper.status.report.table.TableWriterFactory;

import java.io.PrintStream;

public class IngestTaskStatusReport {

    private static final TableWriterFactory.Builder TABLE_FACTORY_BUILDER = TableWriterFactory.builder();

    private static final TableField STATE = TABLE_FACTORY_BUILDER.addField("STATE");
    private static final TableField START_TIME = TABLE_FACTORY_BUILDER.addField("START_TIME");
    private static final TableField FINISH_TIME = TABLE_FACTORY_BUILDER.addField("FINISH_TIME");
    private static final TableField TASK_ID = TABLE_FACTORY_BUILDER.addField("TASK_ID");

    private static final TableWriterFactory TABLE_FACTORY = TABLE_FACTORY_BUILDER.build();

    private final PrintStream out;

    public IngestTaskStatusReport(PrintStream out) {
        this.out = out;
    }

    public void run(IngestTaskQuery query) {
        out.println();
        out.println("Ingest Task Status Report");
        out.println("-------------------------");
        out.printf("Total tasks: %s%n", 0);
        out.printf("Total unfinished tasks: %s%n", 0);
        out.printf("Total finished tasks: %s%n", 0);

        TABLE_FACTORY.tableBuilder()
                .build().write(out);
    }
}

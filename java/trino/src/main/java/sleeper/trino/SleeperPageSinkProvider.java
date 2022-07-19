package sleeper.trino;

import io.trino.spi.connector.*;
import sleeper.trino.handle.SleeperInsertTableHandle;
import sleeper.trino.remotesleeperconnection.SleeperConnectionAsTrino;

import javax.inject.Inject;

/**
 * This class provides {@link SleeperPageSink} classes to support INSERT operations. Note that CREATE TABLE...AS
 * operations are not supported.
 */
public class SleeperPageSinkProvider implements ConnectorPageSinkProvider {
    private final SleeperConnectionAsTrino sleeperConnectionAsTrino;

    @Inject
    public SleeperPageSinkProvider(SleeperConnectionAsTrino sleeperConnectionAsTrino) {
        this.sleeperConnectionAsTrino = sleeperConnectionAsTrino;
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle) {
        throw new UnsupportedOperationException("Writing directly to new tables is not supported.");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle) {
        SleeperInsertTableHandle sleeperInsertTableHandle = (SleeperInsertTableHandle) insertTableHandle;
        return new SleeperPageSink(
                sleeperConnectionAsTrino,
                sleeperInsertTableHandle.getSleeperTableHandle().getSchemaTableName(),
                sleeperInsertTableHandle.getSleeperColumnHandlesInOrder());
    }
}

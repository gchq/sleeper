/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.trino;

import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;

import sleeper.trino.handle.SleeperInsertTableHandle;
import sleeper.trino.remotesleeperconnection.SleeperConnectionAsTrino;

/**
 * Provides page sinks to support INSERT operations. Note that CREATE TABLE...AS operations are not supported.
 */
public class SleeperPageSinkProvider implements ConnectorPageSinkProvider {
    private final SleeperConnectionAsTrino sleeperConnectionAsTrino;

    @Inject
    public SleeperPageSinkProvider(SleeperConnectionAsTrino sleeperConnectionAsTrino) {
        this.sleeperConnectionAsTrino = sleeperConnectionAsTrino;
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle, ConnectorPageSinkId pageSinkId) {
        throw new UnsupportedOperationException("Writing directly to new tables is not supported.");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle, ConnectorPageSinkId pageSinkId) {
        SleeperInsertTableHandle sleeperInsertTableHandle = (SleeperInsertTableHandle) insertTableHandle;
        return new SleeperPageSink(
                sleeperConnectionAsTrino,
                sleeperInsertTableHandle.getSleeperTableHandle().getSchemaTableName(),
                sleeperInsertTableHandle.getSleeperColumnHandlesInOrder());
    }
}

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
package sleeper.query.runner.websocket;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class QueryWebSocketRowsException extends Exception {

    private final long recordsSent;

    public QueryWebSocketRowsException(Exception cause, long recordsSent) {
        super(cause);
        this.recordsSent = recordsSent;
    }

    public long getRecordsSent() {
        return recordsSent;
    }

    @Override
    @SuppressFBWarnings("BC_UNCONFIRMED_CAST_OF_RETURN_VALUE")
    public Exception getCause() {
        return (Exception) super.getCause();
    }

}

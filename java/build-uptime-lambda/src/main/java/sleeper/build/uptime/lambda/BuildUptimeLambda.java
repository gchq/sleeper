/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.build.uptime.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.ec2.Ec2Client;

public class BuildUptimeLambda implements RequestHandler<ScheduledEvent, Void> {

    public static final Logger LOGGER = LoggerFactory.getLogger(BuildUptimeLambda.class);

    private final Ec2Client ec2;

    public BuildUptimeLambda() {
        this(Ec2Client.create());
    }

    public BuildUptimeLambda(Ec2Client ec2) {
        this.ec2 = ec2;
    }

    @Override
    public Void handleRequest(ScheduledEvent event, Context context) {
        LOGGER.info("Received event: {}", event);
        return null;
    }

}

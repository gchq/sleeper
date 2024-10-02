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
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.ec2.Ec2Client;

import java.io.InputStream;
import java.io.OutputStream;

public class BuildUptimeLambda implements RequestStreamHandler {

    public static final Logger LOGGER = LoggerFactory.getLogger(BuildUptimeLambda.class);

    private final Ec2Client ec2;
    private final BuildUptimeEventSerDe serDe = new BuildUptimeEventSerDe();

    public BuildUptimeLambda() {
        this(Ec2Client.create());
    }

    public BuildUptimeLambda(Ec2Client ec2) {
        this.ec2 = ec2;
    }

    @Override
    public void handleRequest(InputStream input, OutputStream output, Context context) {
        BuildUptimeEvent event = serDe.fromJson(input);
        LOGGER.info("Found event: {}", event);

        switch (event.getOperation()) {
            case "start":
                ec2.startInstances(builder -> builder.instanceIds(event.getEc2Ids()));
                break;
            default:
                throw new IllegalArgumentException("Unrecognised operation: " + event.getOperation());
        }
    }

}

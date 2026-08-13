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
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.api.PropertyUpdates;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.Map;
import java.util.Properties;

@Path("/api")
public class InstancePropertiesResource {

    private final S3Client s3Client;
    private final String instanceId;
    private final String accountName;

    @Inject
    public InstancePropertiesResource(
        S3Client s3Client,
        @ConfigProperty(name = "sleeper.instance.id") String instanceId,
        @ConfigProperty(name = "sleeper.account.name") String accountName
    ) {
        this.s3Client = s3Client;
        this.instanceId = instanceId;
        this.accountName = accountName;
    }

    @GET
    @Path("/instance/properties")
    @Produces(MediaType.APPLICATION_JSON)
    public Properties getInstanceProperties() {
        InstanceProperties instanceProperties = S3InstanceProperties.loadGivenAccountAndInstanceId(s3Client, accountName, instanceId);
        return instanceProperties.getProperties();
    }

    @POST
    @Path("/instance/properties")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateInstanceProperties(Map<String, String> changes) {
        InstanceProperties instanceProperties = S3InstanceProperties.loadGivenAccountAndInstanceIdNoValidation(s3Client, accountName, instanceId);
        PropertyUpdates.applyOrThrow(instanceProperties, changes);
        S3InstanceProperties.saveToS3(s3Client, instanceProperties);
        return Response.noContent().build();
    }

}

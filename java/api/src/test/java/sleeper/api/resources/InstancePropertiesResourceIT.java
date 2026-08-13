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

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.util.Map;

import static io.restassured.RestAssured.given;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

@QuarkusTest
@TestProfile(InstancePropertiesResourceIT.Profile.class)
class InstancePropertiesResourceIT {

    static final String CONFIG_BUCKET = "test-config-bucket-for-properties";

    @Inject
    S3Client s3Client;

    public static class Profile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("sleeper.instance.id", CONFIG_BUCKET);
        }
    }

    @BeforeAll
    static void setUp(S3Client s3Client) {
        s3Client.createBucket(CreateBucketRequest.builder().bucket(CONFIG_BUCKET).build());
    }

    private void putInstanceProperties(String contents) {
        s3Client.putObject(
                builder -> builder.bucket(CONFIG_BUCKET).key("instance.properties"),
                RequestBody.fromString(contents));
    }

    private String readInstanceProperties() {
        return s3Client.getObjectAsBytes(builder -> builder.bucket(CONFIG_BUCKET).key("instance.properties"))
                .asUtf8String();
    }

    @Test
    void shouldReturnPropertiesSorted() {
        putInstanceProperties("sleeper.id=my-instance\nsleeper.account=test-account\nsleeper.jars.bucket=my-jars\n");

        given()
                .when().get("/api/instance/properties")
                .then()
                .statusCode(200)
                .body("'sleeper.account'", is("test-account"))
                .body("'sleeper.id'", is("my-instance"))
                .body("'sleeper.jars.bucket'", is("my-jars"));
    }

    @Test
    void shouldReturnPropertiesInAlphabeticalOrder() {
        putInstanceProperties("z.property=last\na.property=first\nm.property=middle\n");

        given()
                .when().get("/api/instance/properties")
                .then()
                .statusCode(200)
                .body("'a.property'", is("first"))
                .body("'m.property'", is("middle"))
                .body("'z.property'", is("last"));
    }

    @Test
    void shouldSaveValidChangeToInstanceProperties() {
        putInstanceProperties("sleeper.config.bucket=" + CONFIG_BUCKET + "\nsleeper.fs.s3a.max-connections=100\n");

        given()
                .contentType(APPLICATION_JSON)
                .body(Map.of("sleeper.fs.s3a.max-connections", "200"))
                .when().post("/api/instance/properties")
                .then()
                .statusCode(204);

        org.junit.jupiter.api.Assertions.assertTrue(
                readInstanceProperties().contains("sleeper.fs.s3a.max-connections=200"));
    }

    @Test
    void shouldReturn400AndNotSaveWhenValueFailsValidation() {
        putInstanceProperties("sleeper.config.bucket=" + CONFIG_BUCKET + "\nsleeper.fs.s3a.max-connections=100\n");
        String before = readInstanceProperties();

        given()
                .contentType(APPLICATION_JSON)
                .body(Map.of("sleeper.fs.s3a.max-connections", "not-a-number"))
                .when().post("/api/instance/properties")
                .then()
                .statusCode(400)
                .body("invalidProperties.name", hasItem("sleeper.fs.s3a.max-connections"));

        org.junit.jupiter.api.Assertions.assertEquals(before, readInstanceProperties());
    }

    @Test
    void shouldReturn400AndNotSaveWhenChangeRequiresCdkRedeploy() {
        putInstanceProperties("sleeper.config.bucket=" + CONFIG_BUCKET + "\nsleeper.retain.infra.after.destroy=true\n");
        String before = readInstanceProperties();

        given()
                .contentType(APPLICATION_JSON)
                .body(Map.of("sleeper.retain.infra.after.destroy", "false"))
                .when().post("/api/instance/properties")
                .then()
                .statusCode(400)
                .body("cdkDeployRequiredProperties", contains("sleeper.retain.infra.after.destroy"));

        org.junit.jupiter.api.Assertions.assertEquals(before, readInstanceProperties());
    }

    @Test
    void shouldReturn400AndNotSaveWhenPropertyIsUnknown() {
        putInstanceProperties("sleeper.config.bucket=" + CONFIG_BUCKET + "\n");
        String before = readInstanceProperties();

        given()
                .contentType(APPLICATION_JSON)
                .body(Map.of("sleeper.made.up.property", "value"))
                .when().post("/api/instance/properties")
                .then()
                .statusCode(400)
                .body("unknownProperties", contains("sleeper.made.up.property"));

        org.junit.jupiter.api.Assertions.assertEquals(before, readInstanceProperties());
    }

    @Test
    void shouldReturn400AndNotSaveWhenPropertyIsNonEditable() {
        putInstanceProperties("sleeper.config.bucket=" + CONFIG_BUCKET + "\nsleeper.id=my-instance\n");
        String before = readInstanceProperties();

        given()
                .contentType(APPLICATION_JSON)
                .body(Map.of("sleeper.id", "other-instance"))
                .when().post("/api/instance/properties")
                .then()
                .statusCode(400)
                .body("nonEditableProperties", hasItem("sleeper.id"));

        org.junit.jupiter.api.Assertions.assertEquals(before, readInstanceProperties());
    }
}

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
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.notNullValue;

@QuarkusTest
class SleeperPropertyDefinitionsResourceTest {

    @Test
    void shouldReturnGroupedInstanceProperties() {
        given()
                .when().get("/api/sleeper/instance/properties")
                .then()
                .statusCode(200)
                .body("size()", greaterThan(0))
                .body("Common.description", notNullValue())
                .body("Common.properties.size()", greaterThan(0));
    }

    @Test
    void shouldIncludeInstancePropertyDetails() {
        given()
                .when().get("/api/sleeper/instance/properties")
                .then()
                .statusCode(200)
                .body("Common.properties[0].name", notNullValue())
                .body("Common.properties[0].description", notNullValue());
    }

    @Test
    void shouldIncludeExpectedInstancePropertyGroups() {
        given()
                .when().get("/api/sleeper/instance/properties")
                .then()
                .statusCode(200)
                .body("$", hasKey("Common"))
                .body("$", hasKey("Ingest"))
                .body("$", hasKey("Compaction"))
                .body("$", hasKey("Query"));
    }

    @Test
    void shouldIncludeSleeperIdProperty() {
        given()
                .when().get("/api/sleeper/instance/properties")
                .then()
                .statusCode(200)
                .body("Common.properties.find { it.name == 'sleeper.id' }.description", notNullValue())
                .body("Common.properties.find { it.name == 'sleeper.id' }.isEditable", equalTo(false))
                .body("Common.properties.find { it.name == 'sleeper.id' }.isRunCdkDeployWhenChanged", equalTo(false));
    }

    @Test
    void shouldIncludeEditableAndCdkFlagsOnEveryInstanceProperty() {
        given()
                .when().get("/api/sleeper/instance/properties")
                .then()
                .statusCode(200)
                .body("collectMany { it.value.properties }", everyItem(hasKey("isEditable")))
                .body("collectMany { it.value.properties }", everyItem(hasKey("isRunCdkDeployWhenChanged")));
    }

    @Test
    void shouldReturnGroupedTableProperties() {
        given()
                .when().get("/api/sleeper/table/properties")
                .then()
                .statusCode(200)
                .body("size()", greaterThan(0));
    }
}

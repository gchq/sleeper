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
package sleeper.api;

import jakarta.ws.rs.WebApplicationException;
import org.junit.jupiter.api.Test;

import sleeper.api.resources.SleeperPropertiesResource.UpdateFailure;
import sleeper.core.properties.instance.CommonProperty;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.testutils.InstancePropertiesTestHelper;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

class PropertyUpdatesTest {

    private static InstanceProperties baseProperties() {
        InstanceProperties p = InstancePropertiesTestHelper.createTestInstanceProperties();
        p.set(CommonProperty.MAXIMUM_CONNECTIONS_TO_S3, "100");
        return p;
    }

    private static UpdateFailure failureBodyOf(WebApplicationException e) {
        return (UpdateFailure) e.getResponse().getEntity();
    }

    private static WebApplicationException catchWebApplicationException(Runnable runnable) {
        return catchThrowableOfType(WebApplicationException.class, runnable::run);
    }

    @Test
    void shouldApplyValidChange() {
        InstanceProperties props = baseProperties();

        PropertyUpdates.applyOrThrow(props, Map.of("sleeper.fs.s3a.max-connections", "200"));

        assertThat(props.get(CommonProperty.MAXIMUM_CONNECTIONS_TO_S3)).isEqualTo("200");
    }

    @Test
    void shouldNoOpForEmptyChanges() {
        InstanceProperties props = baseProperties();

        PropertyUpdates.applyOrThrow(props, Map.of());

        assertThat(props.get(CommonProperty.MAXIMUM_CONNECTIONS_TO_S3)).isEqualTo("100");
    }

    @Test
    void shouldRejectInvalidValueAndNotMutate() {
        InstanceProperties props = baseProperties();

        WebApplicationException e = catchWebApplicationException(() ->
                PropertyUpdates.applyOrThrow(props, Map.of("sleeper.fs.s3a.max-connections", "not-a-number")));

        assertThat(e.getResponse().getStatus()).isEqualTo(400);
        UpdateFailure body = failureBodyOf(e);
        assertThat(body.invalidProperties()).extracting("name")
                .containsExactly("sleeper.fs.s3a.max-connections");
        assertThat(body.cdkDeployRequiredProperties()).isEmpty();
        assertThat(body.nonEditableProperties()).isEmpty();
        assertThat(body.unknownProperties()).isEmpty();
    }

    @Test
    void shouldRejectCdkRequiredChange() {
        InstanceProperties props = baseProperties();
        props.set(CommonProperty.RETAIN_INFRA_AFTER_DESTROY, "true");

        WebApplicationException e = catchWebApplicationException(() ->
                PropertyUpdates.applyOrThrow(props, Map.of("sleeper.retain.infra.after.destroy", "false")));

        assertThat(e.getResponse().getStatus()).isEqualTo(400);
        UpdateFailure body = failureBodyOf(e);
        assertThat(body.cdkDeployRequiredProperties())
                .containsExactly("sleeper.retain.infra.after.destroy");
        assertThat(body.message()).contains("CDK");
    }

    @Test
    void shouldRejectUnknownProperty() {
        InstanceProperties props = baseProperties();

        WebApplicationException e = catchWebApplicationException(() ->
                PropertyUpdates.applyOrThrow(props, Map.of("sleeper.made.up.property", "x")));

        assertThat(e.getResponse().getStatus()).isEqualTo(400);
        UpdateFailure body = failureBodyOf(e);
        assertThat(body.unknownProperties()).containsExactly("sleeper.made.up.property");
    }

    @Test
    void shouldRejectNonEditableProperty() {
        InstanceProperties props = baseProperties();
        props.set(CommonProperty.ID, "my-instance");

        WebApplicationException e = catchWebApplicationException(() ->
                PropertyUpdates.applyOrThrow(props, Map.of("sleeper.id", "other-instance")));

        assertThat(e.getResponse().getStatus()).isEqualTo(400);
        UpdateFailure body = failureBodyOf(e);
        assertThat(body.nonEditableProperties()).contains("sleeper.id");
    }

    @Test
    void shouldReportAllInvalidPropertiesTogether() {
        InstanceProperties props = baseProperties();

        WebApplicationException e = catchWebApplicationException(() -> PropertyUpdates.applyOrThrow(props, Map.of(
                "sleeper.fs.s3a.max-connections", "not-a-number",
                "sleeper.task.runner.timeout.seconds", "-1")));

        UpdateFailure body = failureBodyOf(e);
        assertThat(body.invalidProperties()).extracting("name")
                .containsExactlyInAnyOrder(
                        "sleeper.fs.s3a.max-connections",
                        "sleeper.task.runner.timeout.seconds");
    }

    @Test
    void shouldSkipCdkCheckWhenChangeEqualsOldValue() {
        InstanceProperties props = baseProperties();
        props.set(CommonProperty.RETAIN_INFRA_AFTER_DESTROY, "true");

        PropertyUpdates.applyOrThrow(props, Map.of("sleeper.retain.infra.after.destroy", "true"));

        assertThat(props.get(CommonProperty.RETAIN_INFRA_AFTER_DESTROY)).isEqualTo("true");
    }
}

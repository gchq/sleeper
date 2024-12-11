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
package sleeper.build.notices;

import org.junit.jupiter.api.Test;

import sleeper.build.maven.DependencyVersions;
import sleeper.build.maven.DependencyVersions.Dependency;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CheckNoticesTest {

    @Test
    void shouldFindMissingNotice() {
        String notices = "";
        DependencyVersions versions = DependencyVersions.builder()
                .dependency("org.apache.datasketches", "datasketches-java", "3.3.0")
                .build();
        assertThatThrownBy(() -> CheckNotices.check(notices, versions))
                .isInstanceOfSatisfying(MissingNoticeException.class,
                        e -> assertThat(e.getDependencies()).extracting(Dependency::describe)
                                .containsExactly("org.apache.datasketches:datasketches-java:3.3.0"));
    }

    @Test
    void shouldFindMissingNotices() {
        String notices = "";
        DependencyVersions versions = DependencyVersions.builder()
                .dependency("com.joom.spark", "spark-platform_2.12", "0.4.7")
                .dependency("org.apache.datasketches", "datasketches-java", "3.3.0")
                .build();
        assertThatThrownBy(() -> CheckNotices.check(notices, versions))
                .isInstanceOfSatisfying(MissingNoticeException.class,
                        e -> assertThat(e.getDependencies()).extracting(Dependency::describe)
                                .containsExactly(
                                        "com.joom.spark:spark-platform_2.12:0.4.7",
                                        "org.apache.datasketches:datasketches-java:3.3.0"));
    }

    @Test
    void shouldFindNoticeIsPresent() {
        String notices = "Apache Datasketches (org.apache.datasketches:datasketches-java:3.*)";
        DependencyVersions versions = DependencyVersions.builder()
                .dependency("org.apache.datasketches", "datasketches-java", "3.3.0")
                .build();
        assertThatCode(() -> CheckNotices.check(notices, versions))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldFindJUnitNoticeIsPresent() {
        String notices = "JUnit (org.junit.jupiter:junit-jupiter-*:5.*, org.junit.platform:junit-platform-suite:1.*)";
        DependencyVersions versions = DependencyVersions.builder()
                .dependency("org.junit.jupiter", "junit-jupiter-api", "5.11.3")
                .dependency("org.junit.platform", "junit-platform-suite", "1.11.3")
                .build();
        assertThatCode(() -> CheckNotices.check(notices, versions))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldFindAwsSdkNoticeIsPresent() {
        String notices = "AWS Java SDK v2 (software.amazon.awssdk:*:2.*)";
        DependencyVersions versions = DependencyVersions.builder()
                .dependency("software.amazon.awssdk", "s3", "2.29.31")
                .dependency("software.amazon.awssdk", "dynamodb", "2.29.31")
                .build();
        assertThatCode(() -> CheckNotices.check(notices, versions))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldFindNoticeIsForWrongArtifact() {
        String notices = "AWS Java SDK v2 (software.amazon.awssdk:dynamodb:2.*)";
        DependencyVersions versions = DependencyVersions.builder()
                .dependency("software.amazon.awssdk", "s3", "2.29.31")
                .build();
        assertThatThrownBy(() -> CheckNotices.check(notices, versions))
                .isInstanceOfSatisfying(MissingNoticeException.class,
                        e -> assertThat(e.getDependencies()).extracting(Dependency::describe)
                                .containsExactly("software.amazon.awssdk:s3:2.29.31"));
    }

    @Test
    void shouldFindArtifactIdDoesNotMatchNoticeWildcard() {
        String notices = "JUnit (org.junit.jupiter:junit-jupiter-*:5.*)";
        DependencyVersions versions = DependencyVersions.builder()
                .dependency("org.junit.jupiter", "junit-other", "5.11.3")
                .build();
        assertThatThrownBy(() -> CheckNotices.check(notices, versions))
                .isInstanceOfSatisfying(MissingNoticeException.class,
                        e -> assertThat(e.getDependencies()).extracting(Dependency::describe)
                                .containsExactly("org.junit.jupiter:junit-other:5.11.3"));
    }

}

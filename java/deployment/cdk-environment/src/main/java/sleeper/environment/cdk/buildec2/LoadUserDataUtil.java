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
package sleeper.environment.cdk.buildec2;

import org.apache.commons.io.IOUtils;
import software.amazon.awscdk.Fn;

import java.net.URL;
import java.nio.charset.Charset;
import java.util.Objects;

class LoadUserDataUtil {

    private LoadUserDataUtil() {
        // Prevent instantiation
    }

    static String userData(BuildEC2Parameters params) {
        return params.fillUserDataTemplate(templateString())
                .replace("%write-files-yaml%", writeFilesYaml(params));
    }

    static String writeFilesYaml(BuildEC2Parameters params) {
        if (!params.isNightlyTestEnabled()) {
            return "";
        }
        return resourceString("write-files-nightly-tests.yaml")
                .replace("${nightlyTestSettingsBase64}",
                        Fn.base64(nightlyTestSettingsJson(params)))
                .replace("${crontabBase64}",
                        Fn.base64(crontab(params)));
    }

    static String nightlyTestSettingsJson(BuildEC2Parameters params) {
        String template = resourceString("nightlyTestSettings.json");
        return params.fillUserDataTemplate(template);
    }

    static String crontab(BuildEC2Parameters params) {
        String template = resourceString("crontab");
        return params.fillUserDataTemplate(template);
    }

    private static String templateString() {
        String userData = resourceString("user_data");
        String cloudInit = resourceString("cloud-init.sh");
        return userData.replace("%cloud-init-script%", cloudInit);
    }

    private static String resourceString(String resourcePath) {
        try {
            URL resource = Objects.requireNonNull(LoadUserDataUtil.class.getClassLoader().getResource(resourcePath));
            return IOUtils.toString(resource, Charset.defaultCharset());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load " + resourcePath, e);
        }
    }
}

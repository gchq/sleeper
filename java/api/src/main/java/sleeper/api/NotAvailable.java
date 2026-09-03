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

/**
 * The response body returned when a requested resource is unavailable, for example when the relevant optional stack or
 * feature is not enabled for the instance, or when a specific job or query could not be found.
 *
 * @param error   a machine-readable error code
 * @param message a human-readable description of why the resource is unavailable
 */
public record NotAvailable(String error, String message) {
}

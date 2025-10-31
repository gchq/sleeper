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
use assert_cmd::cargo_bin;
use assert_cmd::prelude::*;
use predicates::prelude::*; // Used for writing assertions

#[test]
fn invalid_region_maxs() {
    let mut cmd = Command::new(cargo_bin!("query"));
    cmd.args([
        "/tmp/output.parquet",
        "/tmp/input.parquet",
        "--row-keys",
        "col1",
        "--row-keys",
        "col2",
        "--region-mins",
        "a",
        "--region-mins",
        "a",
        "--region-maxs",
        "z",
        "--query-mins",
        "b",
        "--query-mins",
        "b",
        "--query-maxs",
        "g",
        "--query-maxs",
        "g",
    ]);
    cmd.assert().failure().stderr(predicate::str::contains(
        "quantity of region maximums != quantity of row key fields",
    ));
}

#[test]
fn invalid_region_mins() {
    let mut cmd = Command::new(cargo_bin!("query"));
    cmd.args([
        "/tmp/output.parquet",
        "/tmp/input.parquet",
        "--row-keys",
        "col1",
        "--row-keys",
        "col2",
        "--region-mins",
        "a",
        "--region-maxs",
        "z",
        "--region-maxs",
        "z",
        "--query-mins",
        "b",
        "--query-mins",
        "b",
        "--query-maxs",
        "g",
        "--query-maxs",
        "g",
    ]);
    cmd.assert().failure().stderr(predicate::str::contains(
        "quantity of region minimums != quantity of row key fields",
    ));
}

#[test]
fn invalid_query_mins() {
    let mut cmd = Command::new(cargo_bin!("query"));
    cmd.args([
        "/tmp/output.parquet",
        "/tmp/input.parquet",
        "--row-keys",
        "col1",
        "--row-keys",
        "col2",
        "--region-mins",
        "a",
        "--region-mins",
        "a",
        "--region-maxs",
        "z",
        "--region-maxs",
        "z",
        "--query-mins",
        "b",
        "--query-maxs",
        "g",
        "--query-maxs",
        "g",
    ]);
    cmd.assert().failure().stderr(predicate::str::contains(
        "quantity of query region minimums != quantity of row key fields",
    ));
}

#[test]
fn invalid_query_maxs() {
    let mut cmd = Command::new(cargo_bin!("query"));
    cmd.args([
        "/tmp/output.parquet",
        "/tmp/input.parquet",
        "--row-keys",
        "col1",
        "--row-keys",
        "col2",
        "--region-mins",
        "a",
        "--region-mins",
        "a",
        "--region-maxs",
        "z",
        "--region-maxs",
        "z",
        "--query-mins",
        "b",
        "--query-mins",
        "b",
        "--query-maxs",
        "g",
    ]);
    cmd.assert().failure().stderr(predicate::str::contains(
        "quantity of query region maximums != quantity of row key fields",
    ));
}

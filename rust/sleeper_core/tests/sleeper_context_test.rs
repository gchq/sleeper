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
use sleeper_core::{simulate_compaction_row_reads, sleeper_context::SleeperContext};
use std::{sync::Arc, time::Duration};

#[tokio::test]
async fn should_report_compaction_values_after_time() {
    // Given
    let sleeper_context = Arc::new(SleeperContext::default());

    // When / Then
    // Should be no reports yet
    assert_eq!(sleeper_context.get_compaction_rows_read("compact_1"), None);
    assert_eq!(sleeper_context.get_compaction_rows_read("compact_2"), None);

    // When
    let ptr = sleeper_context.clone();
    let check = tokio::spawn(async move {
        // Wait for tasks to start
        tokio::time::sleep(Duration::from_secs_f32(0.5)).await;

        // Compactions should exist, but be 0
        assert_eq!(ptr.get_compaction_rows_read("compact_1"), Some(0));
        assert_eq!(ptr.get_compaction_rows_read("compact_2"), Some(0));

        // After 1 second, should see progress
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(ptr.get_compaction_rows_read("compact_1"), Some(10));
        assert_eq!(ptr.get_compaction_rows_read("compact_2"), Some(10));

        // After 2 seconds, should see compact_1 done and compact_2 progress
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(ptr.get_compaction_rows_read("compact_1"), None);
        assert_eq!(ptr.get_compaction_rows_read("compact_2"), Some(20));
    });

    // Launch simulated compactions
    simulate_compaction_row_reads(&sleeper_context).await;

    check.await.unwrap();

    // When simulation finished - Then no reports
    assert_eq!(sleeper_context.get_compaction_rows_read("compact_1"), None);
    assert_eq!(sleeper_context.get_compaction_rows_read("compact_2"), None);
}

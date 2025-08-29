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

use std::path::Path;

/// Converts a [`Path`] reference to an absolute path (if not already absolute)
/// and returns it as a String.
///
/// # Panics
/// If the path can't be made absolute due to not being able to get the current
/// directory or the path is not valid.
pub fn path_absolute<T: ?Sized + AsRef<Path>>(path: &T) -> String {
    std::path::absolute(path).unwrap().to_str().unwrap().into()
}

#[cfg(test)]
mod path_test {
    use crate::path_absolute;

    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    fn cd_to_tmp() {
        std::env::set_current_dir("/tmp").unwrap();
    }

    #[test]
    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    fn relative_path_converts() {
        cd_to_tmp();
        assert_eq!("/tmp/foo/bar/baz.txt", path_absolute("foo/bar/baz.txt"));
    }

    #[test]
    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    fn relative_path_converts_with_one_dot() {
        cd_to_tmp();
        assert_eq!("/tmp/foo/bar/baz.txt", path_absolute("./foo/bar/baz.txt"));
    }

    #[test]
    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    fn relative_path_converts_with_double_dot() {
        cd_to_tmp();
        assert_eq!(
            "/tmp/../foo/bar/baz.txt",
            path_absolute("../foo/bar/baz.txt")
        );
    }

    #[test]
    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    fn absolute_path_unchanged() {
        cd_to_tmp();
        assert_eq!("/tmp/foo/bar", path_absolute("/tmp/foo/bar"));
    }

    #[test]
    #[should_panic(expected = "cannot make an empty path absolute")]
    fn empty_path_panic() {
        let _ = path_absolute("");
    }
}

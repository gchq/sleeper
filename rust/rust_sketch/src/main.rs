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
fn main() {
    // Make a simple byte array sketch
    let mut sketch = rust_sketch::quantiles::byte::new_byte_sketch(1024);

    // Populate it
    for i in b'\x21'..b'\x7e' {
        sketch.pin_mut().update(&[i]);
    }

    println!(
        "Sketch contains {} values, median {} 99th q {}",
        sketch.get_n(),
        std::str::from_utf8(&sketch.get_quantile(0.5, true).unwrap()).unwrap(),
        std::str::from_utf8(&sketch.get_quantile(0.99, true).unwrap()).unwrap()
    );

    // Serialise it and then deserialise it
    let serial = sketch.serialize(0).expect("Serialise fail");

    let recreate =
        rust_sketch::quantiles::byte::byte_deserialize(&serial).expect("Deserialise fail");

    println!(
        "Sketch contains {} values, median {} 99th q {}",
        recreate.get_n(),
        std::str::from_utf8(&recreate.get_quantile(0.5, true).unwrap()).unwrap(),
        std::str::from_utf8(&recreate.get_quantile(0.99, true).unwrap()).unwrap()
    );

    // Check things match!
    assert_eq!(sketch.get_k(), recreate.get_k());
    assert_eq!(sketch.get_n(), recreate.get_n());
    assert!(
        (sketch.get_normalized_rank_error(true) - recreate.get_normalized_rank_error(true)).abs()
            < f64::EPSILON
    );
    assert_eq!(
        sketch.get_quantile(0.4, true).unwrap(),
        recreate.get_quantile(0.4, true).unwrap()
    );
    assert_eq!(
        sketch.get_min_item().unwrap(),
        recreate.get_min_item().unwrap()
    );
    assert!(
        (sketch.get_rank(&[b'f'], true).unwrap() - recreate.get_rank(&[b'f'], true).unwrap()).abs()
            < f64::EPSILON
    );
}

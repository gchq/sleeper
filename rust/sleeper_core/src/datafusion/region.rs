//! Functions for handling file regions. That is a range of key values, e.g. from 'b' - 'h'.
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
use crate::{ColRange, PartitionBound};
use datafusion::{
    logical_expr::{col, lit},
    prelude::Expr,
};
use log::error;
use std::collections::HashMap;

/// Represents a Sleeper partition region.
///
/// A [`Region`] is multi-dimension key range over row-key columns in Sleeper.
/// If a table has only on row-key column then a region is a single row range. A region in a
/// table with two row-key columns would be a rectangle, etc.
#[derive(Debug, Default)]
pub struct SleeperPartitionRegion<'a> {
    pub region: HashMap<String, ColRange<'a>>,
}

impl<'a> SleeperPartitionRegion<'a> {
    /// Create new region.
    #[must_use]
    pub fn new(region: HashMap<String, ColRange<'a>>) -> Self {
        Self { region }
    }

    /// Number of dimensions in region.
    #[must_use]
    pub fn len(&self) -> usize {
        self.region.len()
    }

    /// Empty check
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.region.is_empty()
    }
}

/// Create the `DataFusion` filtering expression from a Sleeper region.
///
/// For each column in the row keys, we look up the partition range for that
/// column and create a expression tree that combines all the various filtering conditions.
impl From<&SleeperPartitionRegion<'_>> for Option<Expr> {
    fn from(value: &SleeperPartitionRegion<'_>) -> Self {
        let mut col_expr: Option<Expr> = None;
        for (name, range) in &value.region {
            let lower_expr = lower_bound_expr(range, name);
            let upper_expr = upper_bound_expr(range, name);
            let expr = match (lower_expr, upper_expr) {
                (Some(l), Some(u)) => Some(l.and(u)),
                (Some(l), None) => Some(l),
                (None, Some(u)) => Some(u),
                (None, None) => None,
            };
            // Combine this column filter with any previous column filter
            if let Some(e) = expr {
                col_expr = match col_expr {
                    Some(original) => Some(original.and(e)),
                    None => Some(e),
                }
            }
        }
        col_expr
    }
}

/// Calculate the upper bound expression on a given [`ColRange`].
///
/// This takes into account the inclusive/exclusive nature of the bound.
///
fn upper_bound_expr(range: &ColRange, name: &String) -> Option<Expr> {
    if let PartitionBound::Unbounded = range.upper {
        None
    } else {
        let max_bound = bound_to_lit_expr(&range.upper);
        if range.upper_inclusive {
            Some(col(name).lt_eq(max_bound))
        } else {
            Some(col(name).lt(max_bound))
        }
    }
}

/// Calculate the lower bound expression on a given [`ColRange`].
///
/// Not all bounds are present, so `None` is returned for the unbounded case.
///
/// This takes into account the inclusive/exclusive nature of the bound.
///
fn lower_bound_expr(range: &ColRange, name: &String) -> Option<Expr> {
    if let PartitionBound::Unbounded = range.lower {
        None
    } else {
        let min_bound = bound_to_lit_expr(&range.lower);
        if range.lower_inclusive {
            Some(col(name).gt_eq(min_bound))
        } else {
            Some(col(name).gt(min_bound))
        }
    }
}

/// Convert a [`PartitionBound`] to an [`Expr`] that can be
/// used in a bigger expression.
///
/// # Panics
/// If bound is [`PartitionBound::Unbounded`] as we can't construct
/// an expression for that.
///
fn bound_to_lit_expr(bound: &PartitionBound) -> Expr {
    match bound {
        PartitionBound::Int32(val) => lit(*val),
        PartitionBound::Int64(val) => lit(*val),
        PartitionBound::String(val) => lit(val.to_owned()),
        PartitionBound::ByteArray(val) => lit(val.to_owned()),
        PartitionBound::Unbounded => {
            error!("Can't create filter expression for unbounded partition range!");
            panic!("Can't create filter expression for unbounded partition range!");
        }
    }
}

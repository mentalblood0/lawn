use std::cmp::Ordering;

use anyhow::{Context, Result};

/// Represents the first index where a condition is satisfied.
///
/// Contains the index, the value at that index, and any additional data
/// associated with the element.
#[derive(Debug)]
pub struct Satisfying<V, A> {
    /// The index where the condition is first satisfied.
    pub index: u64,
    /// The value at the satisfying index.
    pub value: V,
    /// Additional data associated with the element (e.g., metadata).
    pub additional_data: A,
}

/// Result of a partition point search operation.
///
/// Contains information about the first element that satisfies the search condition,
/// along with metadata about whether an exact match was found.
#[derive(Debug)]
pub struct PartitionPoint<V, A> {
    /// The first element that satisfies the search condition.
    pub first_satisfying: Satisfying<V, A>,
    /// Whether an exact match ([`Ordering::Equal`]) was found during the search.
    /// When `true`, there exists at least one element exactly matching the target.
    pub is_exact: bool,
}

impl<V, A> PartitionPoint<V, A> {
    /// Performs a binary search to find the partition point in a sorted range.
    ///
    /// The partition point is the first index where `target_compare(index)` returns
    /// [`Ordering::Greater`] or [`Ordering::Equal`]. This is equivalent to finding the lower bound
    /// of a target value in a sorted collection.
    ///
    /// # Arguments
    ///
    /// * `from_index` - The inclusive starting index of the search range.
    /// * `to_index` - The exclusive ending index of the search range.
    /// * `target_compare` - A function that takes an index and returns:
    ///   - `(Ordering::Less, value, additional_data)` if the element is less than the target
    ///   - `(Ordering::Equal, value, additional_data)` if the element equals the target
    ///   - `(Ordering::Greater, value, additional_data)` if the element is greater than the target
    ///
    /// # Returns
    ///
    /// `Ok(Some(PartitionPoint))` if at least one element satisfies the condition.
    /// `Ok(None)` if no element satisfies the condition (all elements return [`Ordering::Less`]).
    /// `Err` if the comparison function fails.
    ///
    /// # Algorithm
    ///
    /// This uses a binary search variant that:
    /// - Finds the first element where `compare` returns `Greater` or `Equal`
    /// - Tracks whether any exact match (`Equal`) was found via `is_exact`
    /// - Continues searching leftward to find the true first satisfying element
    pub fn new<F>(
        mut from_index: u64,
        mut to_index: u64,
        mut target_compare: F,
    ) -> Result<Option<Self>>
    where
        F: FnMut(u64) -> Result<(Ordering, V, A)>,
    {
        let mut first_satisfying: Option<Satisfying<V, A>> = None;
        let mut is_exact: bool = false;
        while from_index < to_index {
            let mid = from_index + ((to_index - from_index) >> 1);

            match target_compare(mid).with_context(|| {
                format!(
                    "Can not use user-provided function target_compare for comparison with value at index {mid:?}"
                )
            })? {
                (Ordering::Equal, value, additional_data) => {
                    is_exact = true;
                    if first_satisfying
                        .as_ref()
                        .is_none_or(|first_satisfying| first_satisfying.index > mid)
                    {
                        first_satisfying = Some(Satisfying {
                            index: mid,
                            value: value,
                            additional_data,
                        });
                    }
                    to_index = mid;
                }
                (Ordering::Greater, value, additional_data) => {
                    if first_satisfying
                        .as_ref()
                        .is_none_or(|first_satisfying| first_satisfying.index > mid)
                    {
                        first_satisfying = Some(Satisfying {
                            index: mid,
                            value: value,
                            additional_data,
                        });
                    }
                    to_index = mid;
                }
                (Ordering::Less, _, _) => {
                    from_index = mid + 1;
                }
            }
        }

        Ok(first_satisfying.map(|first_satisfying| Self {
            first_satisfying,
            is_exact,
        }))
    }
}

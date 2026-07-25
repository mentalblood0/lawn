use std::{cmp::Ordering, ops::Range};

use anyhow::{Context, Result};

#[derive(Debug)]
pub struct Satisfying<V, A> {
    pub index: u64,
    pub value: V,
    pub additional_data: A,
}

#[derive(Debug)]
pub struct PartitionPoint<V, A> {
    pub first_satisfying: Satisfying<V, A>,
    pub is_exact: bool,
}

impl<V, A> PartitionPoint<V, A> {
    pub fn new<F>(mut range: Range<u64>, mut target_compare: F) -> Result<Option<Self>>
    where
        F: FnMut(u64) -> Result<(Ordering, V, A)>,
    {
        let mut first_satisfying: Option<Satisfying<V, A>> = None;
        let mut is_exact: bool = false;
        while range.start < range.end {
            let mid = range.start + ((range.end - range.start) >> 1);

            match target_compare(mid).with_context(|| {
                format!(
                    "Can not use user-provided function target_compare for comparison with value \
                     at index {mid:?}"
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
                            value,
                            additional_data,
                        });
                    }
                    range.end = mid;
                }
                (Ordering::Greater, value, additional_data) => {
                    if first_satisfying
                        .as_ref()
                        .is_none_or(|first_satisfying| first_satisfying.index > mid)
                    {
                        first_satisfying = Some(Satisfying {
                            index: mid,
                            value,
                            additional_data,
                        });
                    }
                    range.end = mid;
                }
                (Ordering::Less, _, _) => {
                    range.start = mid + 1;
                }
            }
        }

        Ok(first_satisfying.map(|first_satisfying| Self {
            first_satisfying,
            is_exact,
        }))
    }
}

use std::cmp::Ordering;

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

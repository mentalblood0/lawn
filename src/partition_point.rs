use std::cmp::Ordering;

pub struct Satisfying<V> {
    pub index: u64,
    pub value: V,
}

pub struct PartitionPoint<V> {
    pub first_satisfying: Satisfying<V>,
    pub is_exact: bool,
}

impl<V> PartitionPoint<V> {
    pub fn new<F>(
        mut from_index: u64,
        mut to_index: u64,
        mut target_compare: F,
    ) -> Result<Option<Self>, String>
    where
        F: FnMut(u64) -> Result<(Ordering, V), String>,
    {
        let mut first_satisfying: Option<Satisfying<V>> = None;
        let mut is_exact: bool = false;
        while from_index < to_index {
            let mid = from_index + ((to_index - from_index) >> 1);

            match target_compare(mid)? {
                (Ordering::Equal, value) => {
                    is_exact = true;
                    first_satisfying = Some(Satisfying {
                        index: mid,
                        value: value,
                    });
                    to_index = mid;
                }
                (Ordering::Greater, value) => {
                    first_satisfying = Some(Satisfying {
                        index: mid,
                        value: value,
                    });
                    to_index = mid;
                }
                (Ordering::Less, _) => {
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

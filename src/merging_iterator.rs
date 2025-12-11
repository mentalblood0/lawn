use std::cmp::Ordering;

use anyhow::{Error, Result};
use fallible_iterator::FallibleIterator;

use crate::keyvalue::{Key, Value};

pub struct MergingIterator<'a, K: Key, V: Value> {
    pub new_iter: std::collections::btree_map::Range<'a, K, Option<V>>,
    pub old_iter: Box<dyn FallibleIterator<Item = (K, V), Error = Error> + 'a>,
    pub current_new_keyvalue_option: Option<(&'a K, &'a Option<V>)>,
    pub current_old_keyvalue_option: Option<(K, V)>,
}

impl<'a, K: Key, V: Value> MergingIterator<'a, K, V> {
    pub fn new(
        mut new_iter: std::collections::btree_map::Range<'a, K, Option<V>>,
        mut old_iter: Box<dyn FallibleIterator<Item = (K, V), Error = Error> + 'a>,
    ) -> Result<Self> {
        let current_new_keyvalue_option = new_iter.next();
        let current_old_keyvalue_option = old_iter.next()?;
        Ok(Self {
            new_iter,
            old_iter,
            current_new_keyvalue_option,
            current_old_keyvalue_option,
        })
    }
}

impl<'a, K: Key, V: Value> FallibleIterator for MergingIterator<'a, K, V> {
    type Item = (K, V);
    type Error = Error;

    fn next(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        loop {
            match (
                &self.current_new_keyvalue_option,
                &self.current_old_keyvalue_option,
            ) {
                (Some(current_memtable_keyvalue), Some(current_table_index_keyvalue)) => {
                    match current_memtable_keyvalue
                        .0
                        .cmp(&current_table_index_keyvalue.0)
                    {
                        Ordering::Less => {
                            let result = if let Some(value) = current_memtable_keyvalue.1 {
                                Some((current_memtable_keyvalue.0.clone(), value.clone()))
                            } else {
                                None
                            };
                            self.current_new_keyvalue_option = self.new_iter.next();
                            if result.is_some() {
                                return Ok(result);
                            }
                        }
                        Ordering::Greater => {
                            let result = Some(current_table_index_keyvalue.clone());
                            self.current_old_keyvalue_option = self.old_iter.next()?;
                            return Ok(result);
                        }
                        Ordering::Equal => {
                            let result = if let Some(value) = current_memtable_keyvalue.1 {
                                Some((current_memtable_keyvalue.0.clone(), value.clone()))
                            } else {
                                None
                            };
                            self.current_new_keyvalue_option = self.new_iter.next();
                            self.current_old_keyvalue_option = self.old_iter.next()?;
                            if result.is_some() {
                                return Ok(result);
                            }
                        }
                    }
                }
                (Some(current_memtable_keyvalue), None) => {
                    let result = if let Some(value) = current_memtable_keyvalue.1 {
                        Some((current_memtable_keyvalue.0.clone(), value.clone()))
                    } else {
                        None
                    };
                    self.current_new_keyvalue_option = self.new_iter.next();
                    if result.is_some() {
                        return Ok(result);
                    }
                }
                (None, Some(current_table_index_keyvalue)) => {
                    let result = Some(current_table_index_keyvalue.clone());
                    self.current_old_keyvalue_option = self.old_iter.next()?;
                    return Ok(result);
                }
                (None, None) => return Ok(None),
            }
        }
    }
}

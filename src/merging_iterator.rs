use std::cmp::Ordering;

use anyhow::{Context, Error, Result};
use fallible_iterator::FallibleIterator;

use crate::keyvalue::{Key, Value};

/// Merges key-value pairs from two iterators, handling tombstones and ordering.
///
/// The `MergingIterator` combines results from a "new" iterator (e.g., memtable)
/// and an "old" iterator (e.g., table indices). It handles:
/// - Key ordering (forward/backward)
/// - Tombstone deletion (None values from new iterator)
/// - Key overwrite precedence (new over old)
pub struct MergingIterator<'a, K: Key, V: Value> {
    /// Iterator over newer key-value pairs (typically from memtable)
    pub new_iter: Box<dyn Iterator<Item = (&'a K, &'a Option<V>)> + 'a>,
    /// Iterator over older key-value pairs (typically from table indices)
    pub old_iter: Box<dyn FallibleIterator<Item = (K, V), Error = Error> + 'a>,
    /// Current key-value pair from the new iterator, or None if exhausted
    pub current_new_keyvalue_option: Option<(&'a K, &'a Option<V>)>,
    /// Current key-value pair from the old iterator, or None if exhausted
    pub current_old_keyvalue_option: Option<(K, V)>,
    /// Direction of iteration: false for forward (ascending), true for backward (descending)
    pub backwards: bool,
}

impl<'a, K: Key, V: Value> MergingIterator<'a, K, V> {
    /// Creates a new merging iterator from two iterators.
    ///
    /// Initializes the iterator by fetching the first element from both iterators.
    /// The `backwards` parameter controls iteration direction.
    ///
    /// # Arguments
    ///
    /// * `new_iter` - Iterator for newer key-value pairs (memtable)
    /// * `old_iter` - Iterator for older key-value pairs (table indices)
    /// * `backwards` - If true, iterates in descending order; if false, ascending order
    ///
    /// # Errors
    ///
    /// Returns an error if the old iterator cannot be advanced, even to exhaustion.
    pub fn new(
        mut new_iter: Box<dyn Iterator<Item = (&'a K, &'a Option<V>)> + 'a>,
        mut old_iter: Box<dyn FallibleIterator<Item = (K, V), Error = Error> + 'a>,
        backwards: bool,
    ) -> Result<Self> {
        let current_new_keyvalue_option = new_iter.next();
        let current_old_keyvalue_option = old_iter.next().with_context(
            || "Can not propagate old key-value pairs iterator further (even getting nothing)",
        )?;
        Ok(Self {
            new_iter,
            old_iter,
            current_new_keyvalue_option,
            current_old_keyvalue_option,
            backwards,
        })
    }
}

impl<'a, K: Key, V: Value> FallibleIterator for MergingIterator<'a, K, V> {
    type Item = (K, V);
    type Error = Error;

    /// Returns the next key-value pair in the merged sequence.
    ///
    /// The merging logic:
    /// - Compares current keys from both iterators
    /// - Returns the smaller key first (forward) or larger key first (backward)
    /// - When keys are equal, the new iterator's value takes precedence
    /// - A `None` value acts as a tombstone and returns nothing (skipping the key)
    /// - Both iterators advance when keys are equal to avoid duplicates
    fn next(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        loop {
            match (
                &self.current_new_keyvalue_option,
                &self.current_old_keyvalue_option,
            ) {
                // Both iterators have values: compare keys and return appropriately
                (Some(current_memtable_keyvalue), Some(current_table_index_keyvalue)) => {
                    // Determine ordering based on direction (backwards vs forwards)
                    match if self.backwards {
                        current_table_index_keyvalue
                            .0
                            .cmp(current_memtable_keyvalue.0)
                    } else {
                        current_memtable_keyvalue
                            .0
                            .cmp(&current_table_index_keyvalue.0)
                    } {
                        // New key is smaller: advance new iterator
                        Ordering::Less => {
                            let result = if let Some(value) = current_memtable_keyvalue.1 {
                                Some((current_memtable_keyvalue.0.clone(), value.clone()))
                            } else {
                                // Tombstone: return None (skip this key)
                                None
                            };
                            self.current_new_keyvalue_option = self.new_iter.next();
                            if result.is_some() {
                                return Ok(result);
                            }
                        }
                        // Old key is smaller: advance old iterator
                        Ordering::Greater => {
                            let result = Some(current_table_index_keyvalue.clone());
                            self.current_old_keyvalue_option = self.old_iter.next()?;
                            return Ok(result);
                        }
                        // Keys are equal: new value takes precedence
                        Ordering::Equal => {
                            let result = if let Some(value) = current_memtable_keyvalue.1 {
                                Some((current_memtable_keyvalue.0.clone(), value.clone()))
                            } else {
                                // Tombstone: return None (delete key from old iterator)
                                None
                            };
                            self.current_new_keyvalue_option = self.new_iter.next();
                            self.current_old_keyvalue_option = self.old_iter.next().with_context(
                                || "Can not propagate old key-value pairs iterator further (even getting nothing)",
                            )?;
                            if result.is_some() {
                                return Ok(result);
                            }
                        }
                    }
                }
                // Only new iterator has values: process them
                (Some(current_memtable_keyvalue), None) => {
                    let result = if let Some(value) = current_memtable_keyvalue.1 {
                        Some((current_memtable_keyvalue.0.clone(), value.clone()))
                    } else {
                        // Tombstone: return None (skip this key)
                        None
                    };
                    self.current_new_keyvalue_option = self.new_iter.next();
                    if result.is_some() {
                        return Ok(result);
                    }
                }
                // Only old iterator has values: advance it
                (None, Some(current_table_index_keyvalue)) => {
                    let result = Some(current_table_index_keyvalue.clone());
                    self.current_old_keyvalue_option = self.old_iter.next().with_context(
                        || "Can not propagate old key-value pairs iterator further (even getting nothing)",
                    )?;
                    return Ok(result);
                }
                // Both exhausted: iteration complete
                (None, None) => return Ok(None),
            }
        }
    }
}

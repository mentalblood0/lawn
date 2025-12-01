use std::cmp::Ordering;

use fallible_iterator::FallibleIterator;

pub struct MergingIterator<'a> {
    pub new_iter: std::collections::btree_map::Range<'a, Vec<u8>, Option<Vec<u8>>>,
    pub old_iter: Box<dyn FallibleIterator<Item = (Vec<u8>, Vec<u8>), Error = String> + 'a>,
    pub current_new_keyvalue_option: Option<(&'a Vec<u8>, &'a Option<Vec<u8>>)>,
    pub current_old_keyvalue_option: Option<(Vec<u8>, Vec<u8>)>,
}

impl<'a> FallibleIterator for MergingIterator<'a> {
    type Item = (Vec<u8>, Vec<u8>);
    type Error = String;

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

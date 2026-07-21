use std::{borrow::Cow, cmp::Ordering};

use anyhow::{Context, Error, Result};
use fallible_iterator::FallibleIterator;

use crate::keyvalue::{Key, Value};

type KeyValueOptionCows<'a, K, V> = (Cow<'a, K>, Cow<'a, Option<V>>);

pub struct MergingIterator<'a, K: Key, V: Value> {
    pub new_iter: Box<dyn Iterator<Item = KeyValueOptionCows<'a, K, V>> + 'a>,
    pub old_iter: Box<dyn FallibleIterator<Item = (Cow<'a, K>, Cow<'a, V>), Error = Error> + 'a>,
    pub current_new_keyvalue_option: Option<(Cow<'a, K>, Cow<'a, Option<V>>)>,
    pub current_old_keyvalue_option: Option<(Cow<'a, K>, Cow<'a, V>)>,
    pub backwards: bool,
}

impl<'a, K: Key, V: Value> MergingIterator<'a, K, V> {
    pub fn new(
        mut new_iter: Box<dyn Iterator<Item = KeyValueOptionCows<'a, K, V>> + 'a>,
        mut old_iter: Box<
            dyn FallibleIterator<Item = (Cow<'a, K>, Cow<'a, V>), Error = Error> + 'a,
        >,
        backwards: bool,
    ) -> Result<Self> {
        let current_new_keyvalue_option = new_iter.next();
        let current_old_keyvalue_option = old_iter.next().with_context(|| {
            "Can not propagate old key-value pairs iterator further (even getting nothing)"
        })?;
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
    type Item = (Cow<'a, K>, Cow<'a, V>);
    type Error = Error;

    fn next(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        loop {
            match (
                &mut self.current_new_keyvalue_option,
                &mut self.current_old_keyvalue_option,
            ) {
                (Some(current_new_keyvalue), Some(current_old_keyvalue)) => {
                    match if self.backwards {
                        current_old_keyvalue.0.cmp(&current_new_keyvalue.0)
                    } else {
                        current_new_keyvalue.0.cmp(&current_old_keyvalue.0)
                    } {
                        Ordering::Less => {
                            let result =
                                std::mem::replace(&mut current_new_keyvalue.1, Cow::Owned(None))
                                    .into_owned()
                                    .map(|value| {
                                        (current_new_keyvalue.0.clone(), Cow::Owned(value))
                                    });
                            self.current_new_keyvalue_option = self.new_iter.next();
                            if result.is_some() {
                                return Ok(result);
                            }
                        }
                        Ordering::Greater => {
                            let result = Some(current_old_keyvalue.clone());
                            self.current_old_keyvalue_option = self.old_iter.next()?;
                            return Ok(result);
                        }
                        Ordering::Equal => {
                            let result =
                                std::mem::replace(&mut current_new_keyvalue.1, Cow::Owned(None))
                                    .into_owned()
                                    .map(|value| {
                                        (current_new_keyvalue.0.clone(), Cow::Owned(value))
                                    });
                            self.current_new_keyvalue_option = self.new_iter.next();
                            self.current_old_keyvalue_option =
                                self.old_iter.next().with_context(|| {
                                    "Can not propagate old key-value pairs iterator further (even \
                                     getting nothing)"
                                })?;
                            if result.is_some() {
                                return Ok(result);
                            }
                        }
                    }
                }
                (Some(current_new_keyvalue), None) => {
                    let result = std::mem::replace(&mut current_new_keyvalue.1, Cow::Owned(None))
                        .into_owned()
                        .map(|value| (current_new_keyvalue.0.clone(), Cow::Owned(value)));
                    self.current_new_keyvalue_option = self.new_iter.next();
                    if result.is_some() {
                        return Ok(result);
                    }
                }
                (None, Some(current_table_index_keyvalue)) => {
                    let result = Some(current_table_index_keyvalue.clone());
                    self.current_old_keyvalue_option = self.old_iter.next().with_context(|| {
                        "Can not propagate old key-value pairs iterator further (even getting \
                         nothing)"
                    })?;
                    return Ok(result);
                }
                (None, None) => return Ok(None),
            }
        }
    }
}

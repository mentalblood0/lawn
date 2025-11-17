use std::ops::{Add, BitAnd, Shr};

pub trait Betweenable
where
    Self: Copy,
{
    fn between(x: Self, y: Self) -> Option<Self>;
}

impl<X> Betweenable for X
where
    X: Copy,
    X: Shr<i8, Output = X>,
    X: Add<X, Output = X>,
    X: BitAnd<X, Output = X>,
    X: From<u8>,
    X: PartialOrd,
{
    fn between(low: Self, high: Self) -> Option<Self> {
        let one = X::from(1);
        if high <= low + one {
            None
        } else {
            let mid = (low >> 1) + (high >> 1) + (low & high & one);
            Some(mid)
        }
    }
}

pub enum Direction<A, B> {
    Low(A),
    High(B),
}

pub fn binary_search<X, A, B, F>(
    low: (X, A),
    high: (X, B),
    mut f: F,
) -> Result<((X, A), (X, B)), String>
where
    X: Betweenable,
    F: FnMut(X) -> Result<Direction<A, B>, String>,
{
    match X::between(low.0, high.0) {
        None => Ok((low, high)),
        Some(x) => match f(x)? {
            Direction::Low(low) => binary_search((x, low), high, f),
            Direction::High(high) => binary_search(low, (x, high), f),
        },
    }
}

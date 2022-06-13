//! Nested loop join.
use std::iter::Peekable;

pub struct NestedLoopJoin<L: Iterator, R, F> {
    left: Peekable<L>,
    right: R,
    right_curr: R,
    pred: F,
}

impl<L, R, F> NestedLoopJoin<L, R, F>
where
    L: Iterator,
    R: Iterator + Clone,
    F: Fn(&L::Item, &R::Item) -> bool,
{
    pub fn new(left: L, right: R, pred: F) -> Self {
        let right_curr = right.clone();
        NestedLoopJoin {
            left: left.peekable(),
            right,
            right_curr,
            pred,
        }
    }
}

impl<LI, L, R, F> Iterator for NestedLoopJoin<L, R, F>
where
    LI: Clone,
    L: Iterator<Item = LI>,
    R: Iterator + Clone,
    F: Fn(&L::Item, &R::Item) -> bool,
{
    type Item = (L::Item, R::Item);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let left = match self.left.peek() {
                Some(item) => item,
                None => return None,
            };

            while let Some(right) = self.right_curr.next() {
                if (self.pred)(left, &right) {
                    return Some((left.clone(), right));
                }
            }

            let _ = self.left.next();
            self.right_curr = self.right.clone();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equality() {
        let pred = |a: &i32, b: &i32| a == b;

        let l = vec![1, 2, 3, 4, 5];
        let r = vec![2, 3, 4, 5, 6];
        let expected = vec![(2, 2), (3, 3), (4, 4), (5, 5)];

        let nlj = NestedLoopJoin::new(l.into_iter(), r.into_iter(), pred);
        let out: Vec<_> = nlj.collect();

        assert_eq!(expected, out);
    }

    #[test]
    fn fixed_ranges() {
        let pred = |&a: &i32, &b: &i32| b >= a - 1 && b <= a + 1;

        let l = vec![1, 2, 3, 4];
        let r = vec![2, 3, 4, 5];
        let expected = vec![
            (1, 2),
            (2, 2),
            (2, 3),
            (3, 2),
            (3, 3),
            (3, 4),
            (4, 3),
            (4, 4),
            (4, 5),
        ];

        let nlj = NestedLoopJoin::new(l.into_iter(), r.into_iter(), pred);
        let out: Vec<_> = nlj.collect();

        assert_eq!(expected, out);
    }

    #[test]
    fn out_of_order() {
        let pred = |a: &i32, b: &i32| a > b;

        let l = vec![4, 3, 4, 1];
        let r = vec![1, 5, 2];
        let expected = vec![(4, 1), (4, 2), (3, 1), (3, 2), (4, 1), (4, 2)];

        let nlj = NestedLoopJoin::new(l.into_iter(), r.into_iter(), pred);
        let out: Vec<_> = nlj.collect();

        assert_eq!(expected, out);
    }
}

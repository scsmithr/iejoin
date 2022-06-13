//! Inequality join.
use bitvec::vec::BitVec;
use std::fmt::Debug;

#[derive(Debug)]
pub enum CmpOp {
    Lt,   // <
    LtEq, // <=
    Gt,   // >
    GtEq, // >=
}

impl CmpOp {
    fn sort_order(&self) -> SortOrder {
        match self {
            CmpOp::Gt | CmpOp::LtEq => SortOrder::Desc,
            _ => SortOrder::Asc,
        }
    }
}

#[derive(Debug)]
pub struct JoinPredicate<T> {
    op: CmpOp,
    left: Vec<T>,
    right: Vec<T>,
}

impl<T> JoinPredicate<T>
where
    T: Ord,
{
    pub fn new(op: CmpOp, left: Vec<T>, right: Vec<T>) -> Self {
        JoinPredicate { op, left, right }
    }
}

#[derive(Debug)]
pub struct InequalityJoin<T1, T2> {
    l1: L1Union<T1>,
    l2: L2Union<T2>,

    l2_idx: usize,

    bitvec: L1BitVec,
}

impl<T1, T2> InequalityJoin<T1, T2>
where
    T1: Ord + Debug,
    T2: Ord + Debug,
{
    pub fn new(join1: JoinPredicate<T1>, join2: JoinPredicate<T2>) -> Self {
        let (perms, l1) = L1Union::union_and_sort(join1.left, join1.right, join1.op.sort_order());
        let l2 = L2Union::union_and_sort_with_permutations(
            join2.left,
            join2.right,
            join2.op.sort_order(),
            perms,
        );

        let bitvec = L1BitVec::new(l1.values.len());

        InequalityJoin {
            l1,
            l2,
            l2_idx: 0,
            bitvec,
        }
    }
}

impl<T1, T2> Iterator for InequalityJoin<T1, T2>
where
    T1: Ord + Clone + Debug,
    T2: Ord + Clone + Debug,
{
    type Item = (T1, T2);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (v2, permuted) = match self.l2.values.get(self.l2_idx) {
                Some((v, p)) => (v, *p),
                None => return None,
            };
            self.bitvec.maybe_reset_start(permuted);

            let (_, side, _) = self.l1.values.get(permuted).unwrap();
            if side.is_right() {
                self.l2_idx += 1;
                self.bitvec.set(permuted);
                continue;
            }

            if let Some(idx) = self.bitvec.next() {
                let (v1, _, _) = self.l1.values.get(idx).unwrap();
                return Some((v1.clone(), v2.clone()));
            }

            self.l2_idx += 1;
        }
    }
}

#[derive(Debug)]
struct L1BitVec {
    bitvec: BitVec,
    idx: usize,
    start_idx: usize,
}

impl L1BitVec {
    fn new(size: usize) -> L1BitVec {
        L1BitVec {
            bitvec: BitVec::repeat(false, size),
            idx: 0,
            start_idx: 0,
        }
    }

    fn maybe_reset_start(&mut self, start_idx: usize) {
        if start_idx == self.start_idx {
            return;
        }
        self.idx = start_idx;
        self.start_idx = start_idx;
    }

    fn set(&mut self, idx: usize) {
        self.bitvec.set(idx, true);
    }
}

impl Iterator for L1BitVec {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(set) = self.bitvec.get(self.idx) {
            if *set {
                let idx = self.idx;
                self.idx += 1;
                return Some(idx);
            }
            self.idx += 1;
        }
        None
    }
}

enum SortOrder {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq)]
enum Side {
    Left,
    Right,
}

impl Side {
    fn is_right(&self) -> bool {
        matches!(self, Side::Right)
    }
}

#[derive(Debug)]
struct L1Union<T> {
    values: Vec<(T, Side, usize)>,
}

impl<T> L1Union<T>
where
    T: Ord,
{
    fn union_and_sort(left: Vec<T>, right: Vec<T>, order: SortOrder) -> (Vec<usize>, Self) {
        let left = left
            .into_iter()
            .enumerate()
            .map(|(idx, v)| (v, Side::Left, idx));
        let right = right
            .into_iter()
            .enumerate()
            .map(|(idx, v)| (v, Side::Right, idx));

        let mut values: Vec<_> = left.chain(right).collect();
        let mut idxs: Vec<_> = (0..values.len()).collect();

        match order {
            SortOrder::Asc => {
                idxs.sort_unstable_by_key(|&idx| &values[idx].0);
                values.sort_unstable_by(|(v1, _, _), (v2, _, _)| v1.cmp(v2));
            }
            SortOrder::Desc => {
                idxs.sort_unstable_by_key(|&idx| &values[idx].0);
                idxs.reverse();
                values.sort_unstable_by(|(v1, _, _), (v2, _, _)| v2.cmp(v1));
            }
        }

        (idxs, L1Union { values })
    }
}

#[derive(Debug)]
struct L2Union<T> {
    values: Vec<(T, usize)>,
}

impl<T> L2Union<T>
where
    T: Ord,
{
    fn union_and_sort_with_permutations(
        left: Vec<T>,
        right: Vec<T>,
        order: SortOrder,
        permutations: Vec<usize>,
    ) -> Self {
        let left = left.into_iter();
        let right = right.into_iter();

        let mut permutations: Vec<_> = permutations.into_iter().enumerate().collect();

        let mut values: Vec<_> = left
            .chain(right)
            .enumerate()
            .map(|(idx, v)| (v, idx))
            .collect();
        match order {
            SortOrder::Asc => {
                permutations.sort_unstable_by_key(|&(idx, _)| &values[idx].0);
                values.sort_unstable_by(|(v1, _), (v2, _)| v1.cmp(v2));
            }
            SortOrder::Desc => {
                permutations.sort_unstable_by_key(|&(idx, _)| &values[idx].0);
                permutations.reverse();
                values.sort_unstable_by(|(v1, _), (v2, _)| v2.cmp(v1));
            }
        }

        L2Union { values }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple() {
        let join1 = JoinPredicate::new(CmpOp::Gt, vec![100, 140, 80, 90], vec![100, 140, 80, 90]);
        let join2 = JoinPredicate::new(CmpOp::Lt, vec![6, 11, 10, 5], vec![6, 11, 10, 5]);
        let expected = vec![(100, 10), (90, 10)];

        let iejoin = InequalityJoin::new(join1, join2);
        let out: Vec<_> = iejoin.into_iter().collect();
        assert_eq!(expected, out);
    }
}

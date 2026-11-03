use std::collections::HashMap;
use std::hash::Hash;
use std::ops::AddAssign;

#[derive(Debug, Clone)]
pub struct ZSet<T>(pub HashMap<T, i64>);

impl<T> Default for ZSet<T> {
    fn default() -> Self {
        Self(HashMap::new())
    }
}

impl<T: Eq + Hash> ZSet<T> {
    pub fn update(&mut self, row: T, weight: i64) {
        *self.0.entry(row).or_insert(0) += weight;
    }

    pub fn insert(&mut self, row: T) {
        self.update(row, 1);
    }

    pub fn delete(&mut self, row: T) {
        self.update(row, -1);
    }
}

impl<T: Eq + Hash> PartialEq for ZSet<T> {
    fn eq(&self, other: &Self) -> bool {
        let contains = |a: &Self, b: &Self| a.0.iter().all(|(k, w)| *w == 0 || b.0.get(k) == Some(w));
        contains(self, other) && contains(other, self)
    }
}

impl<T: Eq + Hash> Eq for ZSet<T> {}

#[derive(Debug, Clone)]
pub struct Indexed<K, T>(pub HashMap<K, ZSet<T>>);

impl<K, T> Default for Indexed<K, T> {
    fn default() -> Self {
        Self(HashMap::new())
    }
}

impl<K: Eq + Hash + Clone, T: Eq + Hash + Clone> Indexed<K, T> {
    pub fn update(&mut self, key: K, row: T, weight: i64) {
        self.0.entry(key).or_default().update(row, weight);
    }

}

impl<K: Eq + Hash + Clone, T: Eq + Hash + Clone> AddAssign<&Indexed<K, T>> for Indexed<K, T> {
    fn add_assign(&mut self, other: &Self) {
        for (key, rows) in &other.0 {
            for (row, &weight) in &rows.0 {
                self.update(key.clone(), row.clone(), weight);
            }
        }
    }
}

use crate::storage::{Indexed, ZSet};
use serde::{Deserialize, Serialize};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct T1Row {
    pub id1: i64,
    pub x: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct T2Row {
    pub id2: i64,
    pub y: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct JoinRow {
    pub id1: i64,
    pub id2: i64,
    pub x: i64,
    pub y: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub enum DataChangeEvent<Row> {
    Delete(Row),
    Insert(Row),
}

#[derive(Debug, Clone)]
pub enum Change {
    T1(DataChangeEvent<T1Row>),
    T2(DataChangeEvent<T2Row>),
}

pub trait Engine {
    fn update(&mut self, batch: &[Change]) -> Result<(), Error>;
    fn view(&self) -> ZSet<JoinRow>;
}

pub fn join(t1: &Indexed<i64, T1Row>, t2: &Indexed<i64, T2Row>) -> ZSet<JoinRow> {
    let keys: Box<dyn Iterator<Item = &i64>> = if t1.0.len() <= t2.0.len() {
        Box::new(t1.0.keys())
    } else {
        Box::new(t2.0.keys())
    };
    let mut result = ZSet::default();
    for key in keys {
        if let (Some(rows1), Some(rows2)) = (t1.0.get(key), t2.0.get(key)) {
            for (a, &wa) in &rows1.0 {
                for (b, &wb) in &rows2.0 {
                    result.update(JoinRow { id1: a.id1, id2: b.id2, x: a.x, y: b.y }, wa * wb);
                }
            }
        }
    }
    result
}

use crate::DataChangeEvent;
use crate::model::*;
use crate::storage::*;

#[derive(Default)]
pub struct NaiveEngine {
    t1: Indexed<i64, T1Row>,
    t2: Indexed<i64, T2Row>,
}

impl Engine for NaiveEngine {
    fn update(&mut self, batch: &[Change]) -> Result<(), Error> {
        for change in batch {
            match change {
                Change::T1(event) => match event {
                    DataChangeEvent::Insert(row) => self.t1.update(row.id1, row.clone(), 1),
                    DataChangeEvent::Delete(row) => self.t1.update(row.id1, row.clone(), -1),
                },
                Change::T2(event) => match event {
                    DataChangeEvent::Insert(row) => self.t2.update(row.id2, row.clone(), 1),
                    DataChangeEvent::Delete(row) => self.t2.update(row.id2, row.clone(), -1),
                },
            };
        }
        Ok(())
    }

    fn view(&self) -> ZSet<JoinRow> {
        join(&self.t1, &self.t2)
    }
}

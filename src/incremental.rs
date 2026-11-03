use crate::DataChangeEvent;
use crate::model::*;
use crate::storage::*;

#[derive(Default)]
pub struct IncrementalEngine {
    t1: Indexed<i64, T1Row>,
    t2: Indexed<i64, T2Row>,
    view: ZSet<JoinRow>,
}

impl Engine for IncrementalEngine {
    fn update(&mut self, batch: &[Change]) -> Result<(), Error> {
        let mut t1_delta = Indexed::<i64, T1Row>::default();
        let mut t2_delta = Indexed::<i64, T2Row>::default();
        for change in batch {
            match change {
                Change::T1(event) => match event {
                    DataChangeEvent::Insert(row) => t1_delta.update(row.id1, row.clone(), 1),
                    DataChangeEvent::Delete(row) => t1_delta.update(row.id1, row.clone(), -1),
                },
                Change::T2(event) => match event {
                    DataChangeEvent::Insert(row) => t2_delta.update(row.id2, row.clone(), 1),
                    DataChangeEvent::Delete(row) => t2_delta.update(row.id2, row.clone(), -1),
                },
            };
        }

        self.t1 += &t1_delta;

        for (join_row, weight) in join(&self.t1, &t2_delta).0 {
            self.view.update(join_row, weight);
        }

        for (join_row, weight) in join(&t1_delta, &self.t2).0 {
            self.view.update(join_row, weight);
        }

        self.t2 += &t2_delta;

        Ok(())
    }

    fn view(&self) -> ZSet<JoinRow> {
        self.view.clone()
    }
}

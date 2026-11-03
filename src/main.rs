mod feldera;
mod incremental;
mod model;
mod naive;
mod storage;

use model::*;

fn main() -> Result<(), Error> {
    let mut engines: Vec<Box<dyn Engine>> = vec![
        Box::new(feldera::FelderaEngine::new("sql/01.sql")?),
        Box::new(naive::NaiveEngine::default()),
        Box::new(incremental::IncrementalEngine::default()),
    ];
    let updates = [
        Change::T1(DataChangeEvent::Insert(T1Row { id1: 1, x: 2 })),
        Change::T2(DataChangeEvent::Insert(T2Row { id2: 1, y: 3 })),
        Change::T1(DataChangeEvent::Insert(T1Row { id1: 1, x: 6 })),
        Change::T2(DataChangeEvent::Insert(T2Row { id2: 1, y: 7 })),
        Change::T1(DataChangeEvent::Delete(T1Row { id1: 1, x: 2 })),
    ];
    for change in updates {
        let views = engines
            .iter_mut()
            .map(|engine| {
                engine
                    .update(std::slice::from_ref(&change))
                    .map(|_| engine.view())
            })
            .collect::<Result<Vec<_>, _>>()?;
        assert!(
            views.iter().all(|view| *view == views[0]),
            "views differ after {change:?}: {views:?}"
        );
        println!("{change:?} -> {:?}", views[0]);
    }
    Ok(())
}

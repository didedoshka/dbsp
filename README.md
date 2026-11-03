# Название проекта

**Проектное задание:** DBSP/Feldera: инкрементальное выполнение запросов, [ссылка на задание](https://distsys.ru/osukhoroslov/nis2/src/branch/main/project-tasks/p21-dbsp-feldera.md)

**Участники:**

- Горохов Дмитрий
- Понкратов Александр

## Сокращённый запуск

Требования: Rust (edition 2024), Docker, Feldera `pipeline-manager` на `127.0.0.1:8080`.

Запустить Feldera в Docker, затем из каталога `dbsp/` выполнить:

```sh
docker run -p 8080:8080 images.feldera.com/feldera/pipeline-manager:0.375.0
cargo run
```

Описание проверки и ожидаемые результаты — в [материалах КТ-1](nis/checkpoints/01.md).

## Навигация

- [Описание проекта](nis/project.md)
- [План эксперимента](nis/experiment-plan.md)
- [Контрольная точка 1](nis/checkpoints/01.md)
- [Схема инкрементального соединения](nis/incremental-join.svg)
- Контрольные точки 2 и 3, состояние, результаты, финальная версия: ещё не созданы
- Код: [src/](src/)
- Запрос: [sql/01.sql](sql/01.sql)
- Тесты: пока нет
- Данные: пока нет
# EasyDB

EasyDB is an experimental, in-memory database system developed as an AI-assisted educational project. It serves as a playground for learning database internals and SQL processing concepts.

## Purpose
- Educational exploration of database system concepts
- Experiment with modern Java features
- Demonstrate basic database operations in a simplified context

## Key Features
- Pure in-memory storage with no persistence
- SQL parser and query execution engine
- Query planner with support for joins and sorting
- MVCC-based transaction management
- Multiple index types (Planned):
  - B-tree index
  - Hash index
  - GIN (Generalized Inverted Index)

## Note
This is not intended for production use. It's a learning project created with the assistance of AI to understand database internals better.

## Build

```bash
bazel build //:easydb
```

## License
MIT
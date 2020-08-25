# NanoBatcher
__A micro batching library written in Rust.__

Micro-batching is a technique used in processing pipelines where individual tasks are grouped together into small batches. This can improve throughput by reducing the number of requests made to a downstream system. Nano is a micro-batching library.

# Usage
The `NanoBatcher` struct is the interface for submitting batch items as well as shutting down the processing of batches.
The _Nano_ library introduces a `BatchProcessor` trait, which must be implemented for each downstream system receiving batches.

## Documentation
To view the documentation use

```
cargo doc --open
```

## Test
Unit tests can be run with 
```
cargo test
```

## License
MIT © Oliver Daff 
# Redesign of datas3t

## Current Limitations

Currently all processing burden is on a single process, which leads to performance and scalability issues with long-running operations such as:

- Upload of large datasets
- Dataset aggregation tasks
- Concurrent data access and modification
- Resource contention during peak loads

## Proposed Architecture

Since the application is designed to run within Kubernetes, we should adopt a microservices approach to distribute responsibilities and improve scalability:

### Component Separation

1. **API Service (Read-Only Operations)**
   - Handles dataset downloads and statistics
   - Optimized for high-throughput read operations
   - Horizontally scalable based on read traffic

2. **Management Service (Write Operations)**
   - Manages dataset uploads, deletions, and metadata updates
   - Handles S3 cleanup operations
   - Can be scaled independently based on write workload

3. **Aggregation Service**
   - Dedicated to CPU-intensive aggregation tasks
   - Runs as background workers with configurable concurrency
   - Can be scaled based on aggregation queue depth

### Data Storage Improvements

**PostgreSQL Migration**
   - Replace SQLite with PostgreSQL for better concurrency and scalability
   - Leverage PostgreSQL's advanced indexing and query capabilities
   - Enable connection pooling for efficient resource utilization
   - Implement proper transaction management for data consistency

### Additional Enhancements

1. **Metrics and Monitoring**
   - Add Prometheus metrics for each service
   - Implement detailed logging with structured formats
   - Create dashboards for operational visibility

2. **Resource Configuration**
   - Define appropriate resource requests and limits for each component
   - Configure appropriate liveness and readiness probes

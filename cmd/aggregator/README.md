# Datas3t Aggregator

The Datas3t Aggregator is a command-line utility that periodically aggregates smaller dataranges into larger ones for all datasets. This helps optimize storage and query performance.

## Features

- Runs automatically at configurable intervals (default: 30 minutes)
- Processes all datasets in the Datas3t server
- Creates aggregation plans based on datarange size and datapoint count
- Executes aggregation plans by calling the Datas3t server's aggregate endpoint
- Logs all activities for monitoring and debugging

## Configuration

The aggregator can be configured using command-line flags or environment variables:

| Flag | Environment Variable | Description | Default |
|------|---------------------|-------------|---------|
| `--server-url` | `DATAS3T_SERVER_URL` | Datas3t server URL | `http://localhost:8080` |
| `--interval` | `DATAS3T_AGGREGATION_INTERVAL` | Aggregation interval as duration (e.g. 30m, 1h, 2h30m) | `30m` |
| `--target-size` | `DATAS3T_TARGET_DATARANGE_SIZE` | Target datarange size in bytes | `104857600` (100MB) |

## Running the Aggregator

### From Source

```bash
# Run with default configuration
go run cmd/aggregator/main.go

# Run with custom configuration
go run cmd/aggregator/main.go --server-url=http://custom-server:8080 --interval=1h
```

### Using Docker

```bash
# Build the Docker image
docker build -f cmd/aggregator/Dockerfile -t datas3t-aggregator .

# Run the container with default settings
docker run datas3t-aggregator

# Run with custom settings
docker run -e DATAS3T_SERVER_URL=http://your-datas3t-server:8080 -e DATAS3T_AGGREGATION_INTERVAL=2h datas3t-aggregator
```

## Command Line Help

Run the aggregator with the `--help` flag to see all available options:

```bash
$ go run cmd/aggregator/main.go --help
NAME:
   aggregator - Datas3t datarange aggregator utility

USAGE:
   aggregator [global options]

GLOBAL OPTIONS:
   --server-url value       Datas3t server URL (default: "http://localhost:8080") [$DATAS3T_SERVER_URL]
   --interval value         Aggregation interval (e.g. 30m, 1h, 2h30m) (default: 30m) [$DATAS3T_AGGREGATION_INTERVAL]
   --target-size value      Target datarange size in bytes (default: 104857600) [$DATAS3T_TARGET_DATARANGE_SIZE]
   --help, -h               show help
```

## Aggregation Logic

The aggregator uses a hierarchical size-tiered approach to optimize datarange aggregation:

1. Dataranges are categorized into size tiers (starting at 0 bytes, then powers of 10: 10KB, 100KB, etc.)
2. Adjacent ranges within the same tier are merged only when their combined size would promote to a higher tier
3. If no within-tier merges are possible, the algorithm examines adjacent ranges across tiers:
   - It identifies any consecutive ranges, regardless of their tier assignment
   - It merges these ranges only if their combined size would result in a tier higher than any individual range in the group
   - This provides additional optimization opportunities across tier boundaries
4. Ranges in the highest tier are never merged since they can't be promoted further

This approach offers several benefits:
- **Targeted Optimization**: Only performs aggregation when it results in tier promotion
- **Reduced S3 Overhead**: Minimizes small object transfers to S3
- **Hierarchical Growth**: Gradually moves objects up the tier hierarchy
- **Efficient Storage**: Avoids unnecessary merges that don't improve storage characteristics
- **Respects Data Continuity**: Only merges adjacent ranges with sequential key spaces

For detailed documentation, see the [planner README](planner/README.md).

## Project Structure

The aggregator consists of the following components:

- `main.go` - Main CLI application using urfave/cli/v2
- `planner/` - Package for generating aggregation plans
  - `planner.go` - Core logic for creating aggregation plans
  - `planner_test.go` - Unit tests for the planner

## Testing

To run the tests for the planner package:

```bash
cd cmd/aggregator
go test ./planner -v
```

## Integration with Datas3t

The aggregator interacts with the Datas3t server through its API using the following endpoints:

- List datasets: `GET /api/v1/datas3t`
- Get dataranges: `GET /api/v1/datas3t/{dataset_id}/dataranges`
- Aggregate dataranges: `POST /api/v1/datas3t/{dataset_id}/aggregate/{start_key}/{end_key}` 
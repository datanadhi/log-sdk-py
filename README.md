# Data Nadhi Python SDK

## Overview

The Data Nadhi Python SDK is a **logging library** that extends Python's standard logging with:
- **Rule-based routing** to trigger data pipelines
- **Async, non-blocking delivery** to ensure zero impact on application performance
- **Automatic failover** through EchoPost and fallback servers
- **Structured logging** with trace ID support

The SDK behaves like Python's standard `logging` module but adds the ability to route logs to Data Nadhi pipelines based on configurable rules.

---

## Key Features

- **Non-blocking**: Logging calls never block application execution
- **Rule-based routing**: Define conditions to route logs to specific pipelines
- **Multi-level delivery**: Primary server → EchoPost → Fallback server
- **Health monitoring**: Automatic server health checks and recovery
- **Queue management**: Built-in backpressure handling with drain workers
- **Data persistence**: Failed deliveries are stored to disk for recovery
- **Directory isolation**: Multiple independent logging pipelines per process

---

## Installation

Install directly from the repository:

```bash
pip install git+https://github.com/Data-ARENA-Space/data-nadhi-sdk.git
```

---

## Quick Start

### 1. Set up environment variables

```bash
export DATANADHI_API_KEY="your-api-key"
export DATANADHI_SERVER_HOST="http://localhost:5000"  # Optional
```

### 2. Create configuration directory

```bash
mkdir -p .datanadhi/rules
```

### 3. Define rules (`.datanadhi/rules/app.yml`)

```yaml
# Route ERROR logs to error pipeline
- conditions:
    - key: "log_record.level"
      type: "exact"
      value: "ERROR"
  pipelines:
    - "error-handler"
  stdout: true

# Route authenticated user logs to audit pipeline
- conditions:
    - key: "context.user.type"
      type: "exact"
      value: "authenticated"
  pipelines:
    - "audit-pipeline"
```

### 4. Use the logger

```python
from dotenv import load_dotenv
from datanadhi import Logger

load_dotenv()

logger = Logger(module_name="my_app")

logger.info(
    "User login successful",
    context={
        "user": {
            "id": "user123",
            "type": "authenticated",
            "email": "user@example.com"
        }
    }
)

logger.error(
    "Database connection failed",
    context={"database": "postgres", "retry_count": 3}
)
```

---

## How It Works

1. **Log creation**: Logger evaluates log against configured rules
2. **Rule matching**: Determines which pipelines to trigger and whether to print to stdout
3. **Async submission**: Log is enqueued for background processing
4. **Delivery**: Background workers route logs based on server health:
   - Primary server (if healthy)
   - EchoPost (if primary is down)
   - Fallback server (if EchoPost unavailable)
5. **Monitoring**: Health checks run automatically to restore primary routing

---

## Configuration

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATANADHI_API_KEY` | Yes | - | Authentication key for Data Nadhi server |
| `DATANADHI_SERVER_HOST` | No | `http://data-nadhi-server:5000` | Primary server URL |
| `DATANADHI_FALLBACK_SERVER_HOST` | No | `http://datanadhi-fallback-server:5001` | Fallback server URL |
| `DATANADHI_QUEUE_SIZE` | No | `1000` | Async queue max size |
| `DATANADHI_WORKERS` | No | `2` | Number of background workers |
| `DATANADHI_EXIT_TIMEOUT` | No | `5` | Shutdown timeout (seconds) |

### Configuration File (`.datanadhi/config.yml`)

```yaml
server:
  host: http://localhost:5000
  fallback_host: http://localhost:5001

log:
  level: INFO
  datanadhi_log_level: INFO
  stack_level: 0
  skip_stack: 0

async:
  queue_size: 1000
  workers: 2
  exit_timeout: 5

echopost:
  disable: false
```

### Rules Configuration

Rules are defined in `.datanadhi/rules/*.yml` files:

```yaml
- name: "error-logs"
  conditions:
    - key: "log_record.level"
      type: "exact"
      value: "ERROR"
  pipelines:
    - "error-pipeline"
  stdout: true

- name: "admin-actions"
  any_condition_match: false  # AND logic
  conditions:
    - key: "context.user.role"
      type: "exact"
      value: "admin"
    - key: "log_record.level"
      type: "exact"
      value: "INFO"
  pipelines:
    - "admin-audit"
```

**Condition types:**
- `exact`: Exact string match
- `partial`: Substring match
- `regex`: Regular expression match

**Fields:**
- `any_condition_match`: `true` for OR logic, `false` for AND logic (default)
- `negate`: Invert condition result

---

## Logger API

### Initialization

```python
logger = Logger(
    module_name="my_app",           # Optional: module identifier
    handlers=[...],                  # Optional: custom handlers
    datanadhi_dir=".datanadhi",     # Optional: config directory
    log_level=20,                    # Optional: logging level
    echopost_disable=False          # Optional: disable EchoPost
)
```

### Logging Methods

All methods support the same parameters:

```python
logger.debug(message, context={}, trace_id=None, exc_info=False, stack_info=False)
logger.info(message, context={}, trace_id=None, exc_info=False, stack_info=False)
logger.warning(message, context={}, trace_id=None, exc_info=False, stack_info=False)
logger.error(message, context={}, trace_id=None, exc_info=False, stack_info=False)
logger.critical(message, context={}, trace_id=None, exc_info=False, stack_info=False)
logger.exception(message, context={}, trace_id=None)  # Captures exception automatically
```

**Parameters:**
- `message` (str): Log message
- `context` (dict): Structured data to attach to log
- `trace_id` (str): Optional trace ID for request tracking
- `exc_info` (bool): Include exception information
- `stack_info` (bool): Include stack trace
- `**kwargs`: Additional fields merged into context

**Returns:** Internal payload dict if rules are set, None otherwise

### Waiting for Completion

```python
logger.wait_till_logs_pushed()  # Block until all queued logs are processed
```

---

## Architecture

The SDK follows a strict delivery hierarchy:

1. **Primary Server** (HTTP POST) - First choice for log delivery
2. **EchoPost** (Unix socket) - Local buffer when primary is down
3. **Fallback Server** (Batch HTTP POST) - Remote fallback when EchoPost unavailable
4. **Disk Persistence** - Last resort for failed deliveries

### Components

- **Logger**: Main interface for logging
- **Rules Engine**: Evaluates conditions and determines routing
- **SafeQueue**: Thread-safe queue with writeback buffer
- **AsyncProcessor**: Manages worker threads and delivery
- **ServerHealthMonitor**: Tracks server availability
- **DrainWorker**: Activates at 90% queue capacity
- **EchoPost**: Optional local buffer agent

---

## Development

### Prerequisites

- Python 3.8+
- Docker (for EchoPost testing)

### Local Setup

```bash
# Clone repository
git clone https://github.com/Data-ARENA-Space/data-nadhi-sdk.git
cd data-nadhi-sdk

# Install dependencies
pip install -e .

# Run tests
pytest tests/
```
---

## License

Licensed under the [**GNU Affero General Public License v3.0 (AGPLv3)**](LICENSE).

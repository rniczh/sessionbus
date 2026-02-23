# sessionbus

**sessionbus** is a lightweight WebSocket message bus for Python. Clients subscribe by `(user_id, session_id)`; the server pushes payloads to all subscribers for that key. When disconnects, subscriber messages go into an **outbox** (in-memory or PostgreSQL) and are delivered when a client reconnects.

## Key features

- **Simple subscription model**: One key per conversation: `(user_id, session_id)`. Subscribe, send, and receive on that key.
- **Outbox when offline**: If no client is connected for a key, payloads are queued (memory + optional Postgres). On the next subscribe, the server drains the outbox and sends them.
- **Connection pool (client)**: `Client` uses a pool of WebSockets; hash-based routing spreads keys across connections. Reconnect and re-subscribe are handled per slot.

## Installation

From the project root

```bash
pip install -e .
```

With dev dependencies (pytest, pytest-asyncio, pytest-cov). See `[project.optional-dependencies]` in [pyproject.toml](pyproject.toml):

```bash
pip install -e ".[dev]"
```

Optional: Postgres outbox backend:

```bash
pip install -e ".[postgres]"
```

## Usage

**Server:** start the bus and push to a key:

```python
from sessionbus import Server

server = Server(host="127.0.0.1", port=8765)
await server.start()

# Later: push to all subscribers of (user_id, session_id)
await server.push("user-1", "session-abc", payload)
```

For a runnable server script, see [examples/run_server.py](examples/run_server.py).

**Client:** connect, subscribe to the keys you need, then send and receive:

```python
from sessionbus import Client

client = Client("ws://127.0.0.1:8765", pool_size=4)
await client.connect()
await client.subscribe("user-1", "session-abc")

await client.send("Hello", user_id="user-1", session_id="session-abc")
payload = await client.recv("user-1", "session-abc", timeout=5.0)
```

## Development

Run tests:

```bash
pytest
```

## License

sessionbus is released under the MIT License.

"""
Minimal sessionbus server script.

Usage:
    python examples/run_server.py [--host HOST] [--port PORT] [--echo]

Options:
    --host HOST   Bind address (default: 127.0.0.1)
    --port PORT   Port to listen on (default: 8765)
    --echo        Echo every received message back to its sender key
"""

import argparse
import asyncio
import logging

from sessionbus.server import Server
from sessionbus.types import Payload

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


async def main() -> None:
    parser = argparse.ArgumentParser(description="sessionbus server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--echo", action="store_true", help="Echo received message")
    args = parser.parse_args()

    server = Server(host=args.host, port=args.port)

    if args.echo:

        async def on_message(_ws, payload: Payload) -> None:
            await server.push(payload.user_id, payload.session_id, payload)

        server.on_message = on_message

    await server.start()
    logger.info(
        "sessionbus server listening on %s:%d (echo=%s)",
        args.host,
        server.port,
        args.echo,
    )

    try:
        await asyncio.Future()  # run forever
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        await server.stop()
        logger.info("server stopped")


if __name__ == "__main__":
    asyncio.run(main())

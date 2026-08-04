import asyncio
import logging

from dataeng.bronze.bronze_writer import BronzeWriter
from posttrade.ingest.async_ingestor import AsyncIngestor
from posttrade.observability.logging_config import configure_structured_logging

logger = logging.getLogger(__name__)


async def _periodic_flush(writer: BronzeWriter, interval_s: float = 2.0) -> None:
    while True:
        await asyncio.sleep(interval_s)
        writer.flush_due()


async def main() -> None:
    writer = BronzeWriter()
    ingestor = AsyncIngestor(on_raw_message=writer.append)
    flush_task = asyncio.create_task(_periodic_flush(writer))
    try:
        await ingestor.run()
    finally:
        flush_task.cancel()
        writer.flush_all()
        logger.info(
            "bronze writer stopped",
            extra={"total_lines": writer.total_lines_flushed, "total_objects": writer.total_objects_flushed},
        )


if __name__ == "__main__":
    configure_structured_logging()
    asyncio.run(main())

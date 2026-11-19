
import logging
from dataclasses import dataclass

from models.events import KafkaEnvelope
from services.transform import envelope_to_row

logger = logging.getLogger(__name__)


@dataclass
class DlqItem:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    raw: bytes
    error: str


def process_batch(batch: dict) -> tuple[dict[str, list[list]], dict, list[DlqItem]]:
    rows_by_table: dict[str, list[list]] = {}
    offsets: dict = {}
    dlq: list[DlqItem] = []
    for tp, messages in batch.items():
        if not messages:
            continue
        for msg in messages:
            try:
                env = KafkaEnvelope.model_validate_json(msg.value)
                table, row = envelope_to_row(env)
                rows_by_table.setdefault(table, []).append(row)
            except Exception as exc:
                logger.warning('Poison message %s/%s@%s: %s',
                               tp.topic, tp.partition, msg.offset, exc)
                dlq.append(DlqItem(
                    topic=tp.topic, partition=tp.partition, offset=msg.offset,
                    key=msg.key, raw=msg.value, error=str(exc)[:500],
                ))
        offsets[tp] = messages[-1].offset + 1
    return rows_by_table, offsets, dlq

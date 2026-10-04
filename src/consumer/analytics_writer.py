import json
import psycopg2

from src.consumer.config import POSTGRES_CONFIG


class AnalyticsWriter:

    def __init__(self):
        self.conn = psycopg2.connect(**POSTGRES_CONFIG)

    def save_event(
        self,
        operation,
        source_table,
        record_id,
        payload
    ):

        with self.conn.cursor() as cur:

            cur.execute(
                """
                INSERT INTO cdc_events
                (
                    operation,
                    source_table,
                    record_id,
                    payload
                )
                VALUES
                (
                    %s,
                    %s,
                    %s,
                    %s
                )
                """,
                (
                    operation,
                    source_table,
                    record_id,
                    json.dumps(payload)
                )
            )

            self.conn.commit()
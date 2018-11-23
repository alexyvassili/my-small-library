"""Это образец для функций загрузки, обработки и обновления позиций"""

from db import open_db, close_db


class Position:
    def __init__(self, *args, **kwargs):
        pass


def get_positions_ids():
    Q = """SELECT if_id
            FROM table
            WHERE if_kw_status = 2
            ORDER BY if_id
            LIMIT 50;
        """
    cur, conn = open_db()
    cur.execute(Q)
    positions_ids = [item['if_id'] for item in cur]
    close_db(cur, conn)
    return positions_ids


def load_positions(positions_ids):
    Q = """SELECT if_id, if_zgr_orid as ppsys_id, COALESCE(if_item_text, "") as item_text
            FROM table
            WHERE if_id in ({})
            """
    cur, conn = open_db()
    cur.execute(Q.format(",".join(map(str, positions_ids))))
    positions = [Position(**item) for item in cur]
    close_db(cur, conn)
    return positions


def upgrade_table(positions):
    QU = """ UPDATE table
                 SET field = "%s",
                     status_field = 1
                 WHERE id_field = %s
                 ;"""
    cur, conn = open_db()
    for pos in positions:
        cur.execute(QU % (pos.keywords, pos.id))
    conn.commit()
    close_db(cur, conn)


def process_positions(positions):
    for position in positions:
        position.process()

from collections import namedtuple

from src.const import PENGUIN_SCORE_TABLE_NAME, PENGUIN_COLLECTION_SIZE
from src.utils import row_count

TokenRank = namedtuple('TokenRank', 'rarity_score rank percent_rank')

QUERY_STRING = f'SELECT token, rarity_score, RANK() OVER (ORDER BY rarity_score DESC) AS rk, PERCENT_RANK() OVER (ORDER BY rarity_score ASC) AS prk FROM {PENGUIN_SCORE_TABLE_NAME} ORDER BY rarity_score DESC'


def _confirm_score_data_is_populated(con):
    if row_count(con, PENGUIN_SCORE_TABLE_NAME) < PENGUIN_COLLECTION_SIZE:
        raise Exception('Trying to fetch rank data before all data is scored')


def rarity_rank_and_percentiles(con):
    _confirm_score_data_is_populated(con)

    cur = con.execute(QUERY_STRING)
    return dict((row[0], TokenRank(*row[1:])) for row in cur)


def rarity_rank_and_percentile_for_token(con, token):
    _confirm_score_data_is_populated(con)

    cur = con.execute(
        f'SELECT rarity_score, rk, prk FROM ({QUERY_STRING}) WHERE token = ?',
        (token,)
    )
    return TokenRank(*cur.fetchone())

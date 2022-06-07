import heapq
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


def pretty_print_rarest_and_most_common_nfts(rarity_ranks_and_percentiles):
    rarest_nfts = []
    most_common_nfts = []
    for token in rarity_ranks_and_percentiles.keys():
        rank = rarity_ranks_and_percentiles[token].rank
        if len(rarest_nfts) < 15:
            heapq.heappush(rarest_nfts, (-rank, token))
            heapq.heappush(most_common_nfts, (rank, token))
        else:
            heapq.heappushpop(rarest_nfts, (-rank, token))
            heapq.heappushpop(most_common_nfts, (rank, token))

    def pretty_print_rank_stats(token):
        print(
            f'Rank #{rarity_ranks_and_percentiles[token].rank}: {token} with a rarity score of {rarity_ranks_and_percentiles[token].rarity_score}'
        )

    print('\nThe most rare tokens:')
    for _, token in sorted(rarest_nfts, reverse=True):
        pretty_print_rank_stats(token)

    print('\nThe most common tokens:')
    while most_common_nfts:
        _, token = heapq.heappop(most_common_nfts)
        pretty_print_rank_stats(token)

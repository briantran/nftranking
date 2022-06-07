import heapq
from collections import namedtuple

from src.const import PENGUIN_COLLECTION_SIZE
from src.const import PENGUIN_SCORE_TABLE_NAME
from src.utils import row_count

ScoreData = namedtuple('ScoreData', 'rarity_score rank percent_rank')

QUERY_STRING = f'SELECT token, rarity_score, RANK() OVER (ORDER BY rarity_score DESC) AS rk, PERCENT_RANK() OVER (ORDER BY rarity_score ASC) AS prk FROM {PENGUIN_SCORE_TABLE_NAME} ORDER BY rarity_score DESC'


def _confirm_score_data_is_populated(connection):
    if row_count(connection, PENGUIN_SCORE_TABLE_NAME) < PENGUIN_COLLECTION_SIZE:
        raise Exception('Trying to fetch rank data before all data is scored')


def rarity_rank_and_percentiles(connection):
    _confirm_score_data_is_populated(connection)

    score_data_cursor = connection.execute(QUERY_STRING)
    return dict((token, ScoreData(*score_data)) for token, *score_data in score_data_cursor)


def _pretty_print_rank_stats_for_token(token, token_to_score_data):
    score_data = token_to_score_data[token]
    print(
        f'Rank #{score_data.rank}: {token} (Rarity: {score_data.rarity_score}, Percentile: {score_data.percent_rank})'
    )


def pretty_print_rarest_and_most_common_nfts(token_to_score_data):
    rarest_nfts = []
    most_common_nfts = []
    for token, score_data in token_to_score_data.items():
        rank = score_data.rank
        if len(rarest_nfts) < 15:
            heapq.heappush(rarest_nfts, (-rank, token))
            heapq.heappush(most_common_nfts, (rank, token))
        else:
            heapq.heappushpop(rarest_nfts, (-rank, token))
            heapq.heappushpop(most_common_nfts, (rank, token))

    print('\nThe most rare tokens:')
    for _, token in sorted(rarest_nfts, reverse=True):
        _pretty_print_rank_stats_for_token(token, token_to_score_data)

    print('\nThe most common tokens:')
    while most_common_nfts:
        _, token = heapq.heappop(most_common_nfts)
        _pretty_print_rank_stats_for_token(token, token_to_score_data)

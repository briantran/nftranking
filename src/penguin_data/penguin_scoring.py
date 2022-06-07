from collections import defaultdict

from src.const import PENGUIN_TABLE_NAME, PENGUIN_COLLECTION_SIZE, PENGUIN_SCORE_TABLE_NAME
from src.penguin_data.dao import Penguin
from src.utils import row_count, chunks, flush_buffer

DROP_TABLE_STATEMENT = f'DROP TABLE IF EXISTS {PENGUIN_SCORE_TABLE_NAME}'
CREATE_TABLE_STATEMENT = f'CREATE TABLE IF NOT EXISTS {PENGUIN_SCORE_TABLE_NAME} (token int primary key, statistical_score real, rarity_score real)'
INSERT_STATEMENT = f'INSERT INTO {PENGUIN_SCORE_TABLE_NAME} (token, statistical_score, rarity_score) VALUES (?, ?, ?)'
SELECT_UNSCORED_PENGUINS_QUERY = f'SELECT p.* FROM {PENGUIN_TABLE_NAME} p LEFT OUTER JOIN {PENGUIN_SCORE_TABLE_NAME} ps ON p.token=ps.token WHERE ps.token IS NULL'


def _calculate_feature_counts(con):
    feature_count_dict = defaultdict(lambda: defaultdict(int))
    for feature in Penguin.FEATURES:
        feature_counts = con.execute(f'SELECT {feature}, COUNT() FROM {PENGUIN_TABLE_NAME} GROUP BY {feature}')
        for feature_value, count in feature_counts:
            feature_count_dict[feature][feature_value] = count
    return feature_count_dict


def populate_penguin_score_table(con, batch_size, refresh_penguin_scores):
    with con:
        if refresh_penguin_scores:
            con.execute(DROP_TABLE_STATEMENT)

        con.execute(CREATE_TABLE_STATEMENT)

    penguin_score_row_count = row_count(con, PENGUIN_SCORE_TABLE_NAME)
    missing_data = penguin_score_row_count < PENGUIN_COLLECTION_SIZE
    if missing_data:
        if row_count(con, PENGUIN_TABLE_NAME) < PENGUIN_COLLECTION_SIZE:
            raise Exception('Trying to score penguins when not all penguin data is collected')

        feature_count_dict = _calculate_feature_counts(con)

        unscored_penguins = con.execute(SELECT_UNSCORED_PENGUINS_QUERY)
        insertion_buffer = []
        for chunk in chunks(unscored_penguins, batch_size):
            for unscored_penguin_row_data in chunk:
                penguin = Penguin(*unscored_penguin_row_data)
                rarity_score = penguin.rarity_score(feature_count_dict)
                statistical_score = penguin.statistical_score(feature_count_dict)
                insertion_buffer.append((penguin.token, statistical_score, rarity_score))

            flush_buffer(con, INSERT_STATEMENT, insertion_buffer)

    penguin_score_row_count = row_count(con, PENGUIN_SCORE_TABLE_NAME)
    print(f'We have scored {penguin_score_row_count} penguins!')
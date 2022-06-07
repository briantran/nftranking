from collections import defaultdict

from src.const import PENGUIN_COLLECTION_SIZE
from src.const import PENGUIN_SCORE_TABLE_NAME
from src.const import PENGUIN_TABLE_NAME
from src.penguin.dao import Penguin
from src.utils import bulk_insert_statement
from src.utils import chunks
from src.utils import row_count

DROP_TABLE_STATEMENT = f'DROP TABLE IF EXISTS {PENGUIN_SCORE_TABLE_NAME}'
CREATE_TABLE_STATEMENT = f'CREATE TABLE IF NOT EXISTS {PENGUIN_SCORE_TABLE_NAME} (token int primary key, statistical_score real, rarity_score real)'
INSERT_STATEMENT = f'INSERT INTO {PENGUIN_SCORE_TABLE_NAME} (token, statistical_score, rarity_score) VALUES (?, ?, ?)'
SELECT_UNSCORED_PENGUINS_QUERY = f'SELECT p.* FROM {PENGUIN_TABLE_NAME} p LEFT OUTER JOIN {PENGUIN_SCORE_TABLE_NAME} ps ON p.token=ps.token WHERE ps.token IS NULL'


def _calculate_feature_counts(connection):
    """Calculate the number of times each value of each feature has occurred within a collection.

    This will be represented as a 2-dimensional dict: {feature_name => {feature_value => count}}.

    Example:
        {'Face' => {'Eyepatch' => 18}}
    """
    feature_count_dict = defaultdict(lambda: defaultdict(int))
    for feature in Penguin.FEATURES:
        feature_counts_cursor = connection.execute(f'SELECT {feature}, COUNT() FROM {PENGUIN_TABLE_NAME} GROUP BY {feature}')
        for feature_value, count in feature_counts_cursor:
            feature_count_dict[feature][feature_value] = count
    return feature_count_dict


def populate_penguin_score_table(connection, batch_size, refresh_penguin_scores):
    """Calculate and store score data for any NFTs that are not already persisted in PENGUIN_SCORE_TABLE_NAME.
    """
    with connection:
        if refresh_penguin_scores:
            connection.execute(DROP_TABLE_STATEMENT)

        connection.execute(CREATE_TABLE_STATEMENT)

    penguin_score_row_count = row_count(connection, PENGUIN_SCORE_TABLE_NAME)
    missing_data = penguin_score_row_count < PENGUIN_COLLECTION_SIZE
    if missing_data:
        if row_count(connection, PENGUIN_TABLE_NAME) < PENGUIN_COLLECTION_SIZE:
            raise Exception('Trying to score penguins when not all penguin data is collected')

        feature_count_dict = _calculate_feature_counts(connection)

        unscored_penguins_cursor = connection.execute(SELECT_UNSCORED_PENGUINS_QUERY)
        for chunk_of_unscored_penguins in chunks(unscored_penguins_cursor, batch_size):
            penguin_score_insertion_values_list = []
            for unscored_penguin_row_data in chunk_of_unscored_penguins:
                penguin = Penguin(*unscored_penguin_row_data)
                rarity_score = penguin.calculate_rarity_score(feature_count_dict)
                statistical_score = penguin.calculate_statistical_score(feature_count_dict)
                penguin_score_insertion_values_list.append(
                    (penguin.token, statistical_score, rarity_score)
                )

            bulk_insert_statement(connection, INSERT_STATEMENT, penguin_score_insertion_values_list)

    penguin_score_row_count = row_count(connection, PENGUIN_SCORE_TABLE_NAME)
    print(f'We have scored {penguin_score_row_count} penguins!')

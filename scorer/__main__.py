import argparse
import sqlite3
import urllib.request
from collections import defaultdict
import json
import math
from collections import namedtuple

TokenRank = namedtuple('TokenRank', 'rank percent_rank')

DEFAULT_SQL_LITE_DB = 'penguin.db'
PENGUIN_TABLE_NAME = 'penguins'
PENGUIN_SCORE_TABLE_NAME = 'penguin_scores'
COLLECTION_SIZE = 8888
DEFAULT_BATCH_SIZE = 10


# TODO: Split the logic into separate files

class Penguin:
    FEATURES = ('background', 'skin', 'body', 'face', 'head')

    def __init__(self, token, background, skin, body, face, head):
        self.token = token
        self.background = background
        self.skin = skin
        self.body = body
        self.face = face
        self.head = head

    def insert_values(self):
        return [self.token] + [getattr(self, feature) for feature in self.FEATURES]

    def rarity_score(self, feature_count_dict):
        return sum(1 / feature_rarity for feature_rarity in self._feature_rarities(feature_count_dict).values())

    def statistical_score(self, feature_count_dict):
        return math.prod(self._feature_rarities(feature_count_dict).values())

    def _feature_rarity(self, feature, feature_count_dict):
        number_penguins_sharing_trait = feature_count_dict[feature][getattr(self, feature)]
        return number_penguins_sharing_trait / COLLECTION_SIZE

    def _feature_rarities(self, feature_count_dict):
        return dict((feature, self._feature_rarity(feature, feature_count_dict)) for feature in Penguin.FEATURES)

    @staticmethod
    def from_raw_json(token, raw_json):
        data = json.loads(raw_json)
        attributes = data['attributes']
        args = {'token': token}

        feature_dict = dict((attribute['trait_type'].lower(), attribute['value']) for attribute in attributes)
        if set(feature_dict.keys()) != set(Penguin.FEATURES):
            raise Exception(f'{token} has invalid features')

        args.update(feature_dict)
        return Penguin(**args)


def row_count(con, table):
    return con.execute(f'SELECT COUNT() FROM {table}').fetchone()[0]

def flush_buffer(con, insert_statement, insert_buffer):
    if not insert_buffer:
        return

    with con:
        con.executemany(insert_statement, insert_buffer)

    insert_buffer.clear()


def populate_penguin_data_table(con, batch_size):
    with con:
        con.execute(f'CREATE TABLE IF NOT EXISTS {PENGUIN_TABLE_NAME} (token int primary key, background varchar, skin varchar, body varchar, face varchar, head varchar)')

    penguin_data_row_count = row_count(con, PENGUIN_TABLE_NAME)
    missing_data = penguin_data_row_count < COLLECTION_SIZE

    # Fetch missing NFT data
    if missing_data:
        already_stored_tokens = set(row_data[0] for row_data in con.execute('SELECT token FROM penguins'))
        tokens_to_fetch = (token for token in range(COLLECTION_SIZE) if token not in already_stored_tokens)
        # TODO: Make the IO concurrent
        insertion_buffer = []

        feature_string = ', '.join(Penguin.FEATURES)
        insert_statement = f'INSERT INTO {PENGUIN_TABLE_NAME} (token, {feature_string}) VALUES (?, ?, ?, ?, ?, ?)'

        for token in tokens_to_fetch:
            url_string = f'https://ipfs.io/ipfs/QmWXJXRdExse2YHRY21Wvh4pjRxNRQcWVhcKw4DLVnqGqs/{token}'
            with urllib.request.urlopen(url_string) as url:
                raw_json = url.read().decode()
                penguin = Penguin.from_raw_json(token, raw_json)
                insertion_buffer.append(penguin.insert_values())

            if len(insertion_buffer) >= batch_size:
                print(f'Inserting into {PENGUIN_TABLE_NAME}: {[token for token, *_ in insertion_buffer]}')
                flush_buffer(con, insert_statement, insertion_buffer)

        # Flush one more time in case batch
        flush_buffer(con, insert_statement, insertion_buffer)

    penguin_data_row_count = row_count(con, PENGUIN_TABLE_NAME)
    print(f'We have data on {penguin_data_row_count} penguins!')


def populate_penguin_score_table(con, batch_size):
    cur = con.execute(
        f'CREATE TABLE IF NOT EXISTS {PENGUIN_SCORE_TABLE_NAME} (token int primary key, statistical_score real, rarity_score real)')
    penguin_score_row_count = row_count(cur, PENGUIN_SCORE_TABLE_NAME)
    missing_data = penguin_score_row_count < COLLECTION_SIZE
    if missing_data:
        feature_count_dict = defaultdict(lambda: defaultdict(int))
        # TODO: Relying on SQLite to do the counting is easier but may be suboptimal because it scans the table per feature.
        #  Experiment with writing my own logic for scanning the entire table just once if given more time.
        for feature in Penguin.FEATURES:
            feature_counts = cur.execute(f'SELECT {feature}, COUNT() FROM {PENGUIN_TABLE_NAME} GROUP BY {feature}')
            for row in feature_counts:
                feature_value, count = row
                feature_count_dict[feature][feature_value] = count

        foo = cur.execute(
            f'SELECT p.* FROM {PENGUIN_TABLE_NAME} p LEFT OUTER JOIN {PENGUIN_SCORE_TABLE_NAME} ps ON p.token=ps.token WHERE ps.token IS NULL')
        insertion_buffer = []
        insert_statement = f'INSERT INTO {PENGUIN_SCORE_TABLE_NAME} (token, statistical_score, rarity_score) VALUES (?, ?, ?)'
        for row_data in foo:
            penguin = Penguin(*row_data)
            rarity_score = penguin.rarity_score(feature_count_dict)
            statistical_score = penguin.statistical_score(feature_count_dict)
            insertion_buffer.append((penguin.token, statistical_score, rarity_score))

            if len(insertion_buffer) >= batch_size:
                flush_buffer(con, insert_statement, insertion_buffer)

        # Flush one more time in case batch
        flush_buffer(con, insert_statement, insertion_buffer)

    penguin_score_row_count = row_count(cur, PENGUIN_SCORE_TABLE_NAME)
    print(f'We have scored {penguin_score_row_count} penguins!')


def calculate_rarity_rank_and_percentiles(con):
    cur = con.execute(
        f'SELECT token, RANK() OVER (ORDER BY rarity_score DESC), PERCENT_RANK() OVER (ORDER BY rarity_score ASC) FROM {PENGUIN_SCORE_TABLE_NAME} ORDER BY rarity_score DESC')
    return dict((row[0], TokenRank(*row[1:])) for row in cur)


def calculate_rarity_rank_and_percentile_of_token(con, token):
    cur = con.execute(
        f'SELECT rk, prk FROM (SELECT token, RANK() OVER (ORDER BY rarity_score DESC) AS rk, PERCENT_RANK() OVER (ORDER BY rarity_score ASC) AS prk FROM {PENGUIN_SCORE_TABLE_NAME} ORDER BY rarity_score DESC) WHERE token = ?',
        (token,)
    )
    return TokenRank(*cur.fetchone())


def main():
    parser = argparse.ArgumentParser(description="NFT Ranking")
    parser.add_argument('--refresh-penguin-data', action='store_true')
    parser.add_argument('--refresh-penguin-scores', action='store_true')
    parser.add_argument('--db-file', default=DEFAULT_SQL_LITE_DB)
    parser.add_argument('--batch-size', type=int, default=DEFAULT_BATCH_SIZE)
    args = parser.parse_args()

    con = sqlite3.connect(args.db_file)

    with con:
        if args.refresh_penguin_data:
            con.execute(f'DROP TABLE IF EXISTS {PENGUIN_TABLE_NAME}')

        if args.refresh_penguin_scores:
            con.execute(f'DROP TABLE IF EXISTS {PENGUIN_SCORE_TABLE_NAME}')

    # Fetch data
    populate_penguin_data_table(con, args.batch_size)

    # Score them
    populate_penguin_score_table(con, args.batch_size)

    # Calculate percentiles
    token_to_rarity_score_percentile = calculate_rarity_rank_and_percentiles(con)

    # TODO Remove eye-balling code
    for row_data in con.execute(f'SELECT * FROM {PENGUIN_SCORE_TABLE_NAME} ORDER BY rarity_score DESC'):
        print(row_data, token_to_rarity_score_percentile[row_data[0]])
        print(calculate_rarity_rank_and_percentile_of_token(con, row_data[0]))
        # import pdb; pdb.set_trace()

    # Compare the scores against other tools  (ex: https://gem.xyz, https://raritysniper.com, …)

    # Bonus: analyze the correlation between rarity score and 2ndary sales transaction volume and price on a NFT
    # marketplace of your choice.


if __name__ == "__main__":
    main()

import math
import logging

import asyncio
import aiohttp

from collections import defaultdict
from collections import namedtuple

from .const import DATA_FETCH_SEMAPHORE_VALUE
from .const import PENGUIN_COLLECTION_SIZE
from .const import PENGUIN_SCORE_TABLE_NAME
from .const import PENGUIN_TABLE_NAME

from .utils import chunks
from .utils import row_count
from .utils import flush_buffer

TokenRank = namedtuple('TokenRank', 'rank percent_rank')

logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO)


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
        return number_penguins_sharing_trait / PENGUIN_COLLECTION_SIZE

    def _feature_rarities(self, feature_count_dict):
        return dict((feature, self._feature_rarity(feature, feature_count_dict)) for feature in Penguin.FEATURES)

    @staticmethod
    def from_json(token, json_struct):
        attributes = json_struct['attributes']
        args = {'token': token}

        feature_dict = dict((attribute['trait_type'].lower(), attribute['value']) for attribute in attributes)
        if set(feature_dict.keys()) != set(Penguin.FEATURES):
            raise Exception(f'{token} has invalid features')

        args.update(feature_dict)
        return Penguin(**args)


async def fetch_all_token_metadata(tokens):
    sem = asyncio.Semaphore(DATA_FETCH_SEMAPHORE_VALUE)

    async def fetch_token_metadata(session, token):
        async with sem:
            url = f'https://ipfs.io/ipfs/QmWXJXRdExse2YHRY21Wvh4pjRxNRQcWVhcKw4DLVnqGqs/{token}'
            async with session.get(url) as response:
                return await response.json()

    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(
            *(fetch_token_metadata(session, token) for token in tokens),
            return_exceptions=False
        )
    return [Penguin.from_json(token, result).insert_values() for token, result in zip(tokens, results)]


def populate_penguin_data_table(con, batch_size, refresh_penguin_data=False):
    with con:
        if refresh_penguin_data:
            con.execute(f'DROP TABLE IF EXISTS {PENGUIN_TABLE_NAME}')

        con.execute(f'CREATE TABLE IF NOT EXISTS {PENGUIN_TABLE_NAME} (token int primary key, background varchar, skin varchar, body varchar, face varchar, head varchar)')

    penguin_data_row_count = row_count(con, PENGUIN_TABLE_NAME)
    missing_data = penguin_data_row_count < PENGUIN_COLLECTION_SIZE

    # Fetch missing NFT data
    if missing_data:
        already_stored_tokens = set(row_data[0] for row_data in con.execute('SELECT token FROM penguins'))
        tokens_to_fetch = (token for token in range(PENGUIN_COLLECTION_SIZE) if token not in already_stored_tokens)
        insertion_buffer = []

        feature_string = ', '.join(Penguin.FEATURES)
        insert_statement = f'INSERT INTO {PENGUIN_TABLE_NAME} (token, {feature_string}) VALUES (?, ?, ?, ?, ?, ?)'

        for chunk in chunks(tokens_to_fetch, batch_size):
            insertion_buffer.extend(asyncio.run(fetch_all_token_metadata(chunk)))

            logging.info(f'Inserting into {PENGUIN_TABLE_NAME}: {[token for token, *_ in insertion_buffer]}')
            flush_buffer(con, insert_statement, insertion_buffer)

    penguin_data_row_count = row_count(con, PENGUIN_TABLE_NAME)
    logging.info(f'We have data on {penguin_data_row_count} penguins!')


def populate_penguin_score_table(con, batch_size, refresh_penguin_scores):
    with con:
        if refresh_penguin_scores:
            con.execute(f'DROP TABLE IF EXISTS {PENGUIN_SCORE_TABLE_NAME}')

        con.execute(
            f'CREATE TABLE IF NOT EXISTS {PENGUIN_SCORE_TABLE_NAME} (token int primary key, statistical_score real, rarity_score real)')
    penguin_score_row_count = row_count(con, PENGUIN_SCORE_TABLE_NAME)
    missing_data = penguin_score_row_count < PENGUIN_COLLECTION_SIZE
    if missing_data:
        if row_count(con, PENGUIN_TABLE_NAME) < PENGUIN_COLLECTION_SIZE:
            raise Exception('Trying to score penguins when not all penguin data is collected')

        feature_count_dict = defaultdict(lambda: defaultdict(int))
        for feature in Penguin.FEATURES:
            feature_counts = con.execute(f'SELECT {feature}, COUNT() FROM {PENGUIN_TABLE_NAME} GROUP BY {feature}')
            for feature_value, count in feature_counts:
                feature_count_dict[feature][feature_value] = count

        logging.debug(f'Feature Count Dict: {feature_count_dict}')

        unscored_penguins = con.execute(
            f'SELECT p.* FROM {PENGUIN_TABLE_NAME} p LEFT OUTER JOIN {PENGUIN_SCORE_TABLE_NAME} ps ON p.token=ps.token WHERE ps.token IS NULL')
        insertion_buffer = []
        insert_statement = f'INSERT INTO {PENGUIN_SCORE_TABLE_NAME} (token, statistical_score, rarity_score) VALUES (?, ?, ?)'
        for chunk in chunks(unscored_penguins, batch_size):
            for unscored_penguin_row_data in chunk:
                penguin = Penguin(*unscored_penguin_row_data)
                rarity_score = penguin.rarity_score(feature_count_dict)
                statistical_score = penguin.statistical_score(feature_count_dict)
                insertion_buffer.append((penguin.token, statistical_score, rarity_score))

            flush_buffer(con, insert_statement, insertion_buffer)

    penguin_score_row_count = row_count(con, PENGUIN_SCORE_TABLE_NAME)
    logging.info(f'We have scored {penguin_score_row_count} penguins!')


def rarity_rank_and_percentiles(con):
    if row_count(con, PENGUIN_SCORE_TABLE_NAME) < PENGUIN_COLLECTION_SIZE:
        raise Exception('Trying to fetch rank data before all data is scored')

    cur = con.execute(
        f'SELECT token, RANK() OVER (ORDER BY rarity_score DESC), PERCENT_RANK() OVER (ORDER BY rarity_score ASC) FROM {PENGUIN_SCORE_TABLE_NAME} ORDER BY rarity_score DESC')
    return dict((row[0], TokenRank(*row[1:])) for row in cur)


def rarity_rank_and_percentile_for_token(con, token):
    if row_count(con, PENGUIN_SCORE_TABLE_NAME) < PENGUIN_COLLECTION_SIZE:
        raise Exception('Trying to fetch rank data before all data is scored')

    cur = con.execute(
        f'SELECT rk, prk FROM (SELECT token, RANK() OVER (ORDER BY rarity_score DESC) AS rk, PERCENT_RANK() OVER (ORDER BY rarity_score ASC) AS prk FROM {PENGUIN_SCORE_TABLE_NAME} ORDER BY rarity_score DESC) WHERE token = ?',
        (token,)
    )
    return TokenRank(*cur.fetchone())



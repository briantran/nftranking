import asyncio

import aiohttp

from src.const import DATA_FETCH_SEMAPHORE_VALUE
from src.const import PENGUIN_COLLECTION_SIZE
from src.const import PENGUIN_TABLE_NAME
from src.penguin.dao import Penguin
from src.utils import bulk_insert_statement
from src.utils import chunks
from src.utils import row_count

DROP_TABLE_STATEMENT = f'DROP TABLE IF EXISTS {PENGUIN_TABLE_NAME}'
CREATE_TABLE_STATEMENT = f'CREATE TABLE IF NOT EXISTS {PENGUIN_TABLE_NAME} (token int primary key, background varchar, skin varchar, body varchar, face varchar, head varchar)'
PENGUIN_FEATURE_STRING = ', '.join(Penguin.FEATURES)
INSERT_STATEMENT = f'INSERT INTO {PENGUIN_TABLE_NAME} (token, {PENGUIN_FEATURE_STRING}) VALUES (?, ?, ?, ?, ?, ?)'


async def _fetch_all_penguin_insert_values(tokens):
    # Use a semaphore to throttle network requests
    semaphore = asyncio.Semaphore(DATA_FETCH_SEMAPHORE_VALUE)

    async with aiohttp.ClientSession() as session:
        async def fetch_penguin_insert_values(token):
            async with semaphore:
                url = f'https://ipfs.io/ipfs/QmWXJXRdExse2YHRY21Wvh4pjRxNRQcWVhcKw4DLVnqGqs/{token}'
                async with session.get(url) as response:
                    result = await response.json()
                    return Penguin.from_json(token, result).calculate_insert_values()

        return await asyncio.gather(
            *(fetch_penguin_insert_values(token) for token in tokens),
            return_exceptions=False
        )


def populate_penguin_data_table(connection, batch_size, refresh_penguin_data=False):
    with connection:
        if refresh_penguin_data:
            connection.execute(DROP_TABLE_STATEMENT)

        connection.execute(CREATE_TABLE_STATEMENT)

    penguin_data_row_count = row_count(connection, PENGUIN_TABLE_NAME)

    if penguin_data_row_count < PENGUIN_COLLECTION_SIZE:
        print(f'Fetching data for {PENGUIN_COLLECTION_SIZE - penguin_data_row_count} penguins!')

        already_stored_tokens_cursor = connection.execute(f'SELECT token FROM {PENGUIN_TABLE_NAME}')
        already_stored_tokens = set(token for token, *_ in already_stored_tokens_cursor)
        tokens_to_fetch = (token for token in range(PENGUIN_COLLECTION_SIZE) if token not in already_stored_tokens)

        for chunk_of_tokens_to_fetch in chunks(tokens_to_fetch, batch_size):
            penguin_insert_values_list = asyncio.run(_fetch_all_penguin_insert_values(chunk_of_tokens_to_fetch))
            bulk_insert_statement(connection, INSERT_STATEMENT, penguin_insert_values_list)

    penguin_data_row_count = row_count(connection, PENGUIN_TABLE_NAME)
    print(f'We have data on {penguin_data_row_count} penguins!')

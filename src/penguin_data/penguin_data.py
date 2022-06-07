import asyncio

import aiohttp

from src.const import DATA_FETCH_SEMAPHORE_VALUE, PENGUIN_TABLE_NAME, PENGUIN_COLLECTION_SIZE
from src.penguin_data.dao import Penguin
from src.utils import chunks, flush_buffer, row_count

DROP_TABLE_STATEMENT = f'DROP TABLE IF EXISTS {PENGUIN_TABLE_NAME}'
CREATE_TABLE_STATEMENT = f'CREATE TABLE IF NOT EXISTS {PENGUIN_TABLE_NAME} (token int primary key, background varchar, skin varchar, body varchar, face varchar, head varchar)'

PENGUIN_FEATURE_STRING = ', '.join(Penguin.FEATURES)
INSERT_STATEMENT = f'INSERT INTO {PENGUIN_TABLE_NAME} (token, {PENGUIN_FEATURE_STRING}) VALUES (?, ?, ?, ?, ?, ?)'


async def _fetch_all_token_metadata(tokens):
    sem = asyncio.Semaphore(DATA_FETCH_SEMAPHORE_VALUE)

    async with aiohttp.ClientSession() as session:
        async def fetch_token_metadata(token):
            async with sem:
                url = f'https://ipfs.io/ipfs/QmWXJXRdExse2YHRY21Wvh4pjRxNRQcWVhcKw4DLVnqGqs/{token}'
                async with session.get(url) as response:
                    return await response.json()

        results = await asyncio.gather(
            *(fetch_token_metadata(token) for token in tokens),
            return_exceptions=False
        )
    return [Penguin.from_json(token, result).insert_values() for token, result in zip(tokens, results)]


def populate_penguin_data_table(con, batch_size, refresh_penguin_data=False):
    with con:
        if refresh_penguin_data:
            con.execute(DROP_TABLE_STATEMENT)

        con.execute(CREATE_TABLE_STATEMENT)

    penguin_data_row_count = row_count(con, PENGUIN_TABLE_NAME)

    if penguin_data_row_count < PENGUIN_COLLECTION_SIZE:
        print(f'Fetching data for {PENGUIN_COLLECTION_SIZE - penguin_data_row_count} penguins!')
        already_stored_tokens = set(row_data[0] for row_data in con.execute(f'SELECT token FROM {PENGUIN_TABLE_NAME}'))
        tokens_to_fetch = (token for token in range(PENGUIN_COLLECTION_SIZE) if token not in already_stored_tokens)
        insertion_buffer = []

        for chunk in chunks(tokens_to_fetch, batch_size):
            insertion_buffer.extend(asyncio.run(_fetch_all_token_metadata(chunk)))
            flush_buffer(con, INSERT_STATEMENT, insertion_buffer)

    penguin_data_row_count = row_count(con, PENGUIN_TABLE_NAME)
    print(f'We have data on {penguin_data_row_count} penguins!')

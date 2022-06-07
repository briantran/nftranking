import argparse
import json
import sqlite3

from src import penguin_data
from src.const import DEFAULT_SQL_LITE_DB
from src.const import DEFAULT_BATCH_SIZE


def main():
    parser = argparse.ArgumentParser(description="NFT Ranking")
    parser.add_argument(
        '--refresh-penguin-data',
        action='store_true',
        help='Deletes already persisted penguin data (if it exists) and re-fetches.'
    )
    parser.add_argument(
        '--refresh-penguin-scores',
        action='store_true',
        help='Deletes already persisted penguin scores (if it exists) and recalculates.'
    )
    parser.add_argument(
        '--db-file',
        default=DEFAULT_SQL_LITE_DB,
        help='File path of SQLite DB. Use `:memory:` for in-memory store.'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help='Max number of rows inserted into the database at a time.'
    )
    args = parser.parse_args()

    print(f'Connecting to database: {json.dumps(args.db_file)}')
    connection = None
    try:
        connection = sqlite3.connect(args.db_file)

        # Fetch NFT data
        penguin_data.populate_penguin_data_table(connection, args.batch_size, args.refresh_penguin_data)

        # Score NFTs
        penguin_data.populate_penguin_score_table(connection, args.batch_size, args.refresh_penguin_scores)

        # Show stats on rarest and most common NFTs
        token_to_score_data = penguin_data.fetch_token_to_score_data(connection)
        penguin_data.pretty_print_rarest_and_most_common_nfts(token_to_score_data)

        # Kick off interactive experience
        print('\nEnter any token you\'d like stats for: (Use Ctrl+C to quit)')
        while True:
            try:
                user_input = input('--> ')
                token = int(user_input)
                score_data = token_to_score_data[token]
                print('\n'.join((
                    f'Rank: {score_data.rank}',
                    f'Rarity Score: {score_data.rarity_score}',
                    f'Percentile Score: {score_data.percent_rank}',
                    f'Rarity Sniper: https://raritysniper.com/pudgy-penguins/{token}',
                    f'Gem: https://www.gem.xyz/asset/0xbd3531da5cf5857e7cfaa92426877b022e612cf8/{token}',
                )))
            except KeyboardInterrupt:
                print('\nGoodbye!')
                break
            except (KeyError, ValueError):
                print('{} is not a valid token.\nPlease try again.'.format(json.dumps(user_input)))
    finally:
        if connection:
            connection.close()


if __name__ == "__main__":
    main()

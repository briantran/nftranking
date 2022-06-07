import argparse
import json
import sqlite3
from src import penguin_data
from src.const import DEFAULT_SQL_LITE_DB
from src.const import DEFAULT_BATCH_SIZE
from src.const import PENGUIN_SCORE_TABLE_NAME


def main():
    parser = argparse.ArgumentParser(description="NFT Ranking")
    parser.add_argument('--refresh-penguin-data', action='store_true')
    parser.add_argument('--refresh-penguin-scores', action='store_true')
    parser.add_argument('--db-file', default=DEFAULT_SQL_LITE_DB)
    parser.add_argument('--batch-size', type=int, default=DEFAULT_BATCH_SIZE)
    args = parser.parse_args()

    print(f'Connecting to database: {json.dumps(args.db_file)}')
    con = sqlite3.connect(args.db_file)

    # Fetch data
    penguin_data.populate_penguin_data_table(con, args.batch_size, args.refresh_penguin_data)

    # Score them
    penguin_data.populate_penguin_score_table(con, args.batch_size, args.refresh_penguin_scores)

    # Show some interesting stats
    rarity_ranks_and_percentiles = penguin_data.rarity_rank_and_percentiles(con)
    print('\nThe most rare tokens:')
    for token, rarity_score in con.execute(f'SELECT token, rarity_score FROM {PENGUIN_SCORE_TABLE_NAME} ORDER BY rarity_score DESC LIMIT 15'):
        print(f'Rank #{rarity_ranks_and_percentiles[token].rank}: {token} with a rarity score of {rarity_score}')

    print('\nThe most common tokens:')
    for token, rarity_score in con.execute(f'SELECT token, rarity_score FROM {PENGUIN_SCORE_TABLE_NAME} ORDER BY rarity_score ASC LIMIT 15'):
        print(f'Rank #{rarity_ranks_and_percentiles[token].rank}: {token} with a rarity score of {rarity_score}')

    print('\nEnter any token you\'d like stats for:')
    while True:
        try:
            user_input = input('--> ')
            token = int(user_input)
            print('\n'.join([
                f'Rank: {rarity_ranks_and_percentiles[token].rank}',
                f'Rarity Score: {rarity_ranks_and_percentiles[token].rarity_score}',
                f'Percentile Score: {rarity_ranks_and_percentiles[token].percent_rank}',
                f'Rarity Sniper: https://raritysniper.com/pudgy-penguins/{token}',
                f'Gem: https://www.gem.xyz/asset/0xbd3531da5cf5857e7cfaa92426877b022e612cf8/{token}',
            ]))
        except KeyboardInterrupt:
            break
        except (KeyError, ValueError):
            print('{} is not a valid token.\nPlease try again.'.format(json.dumps(user_input)))

    # Bonus: analyze the correlation between rarity score and 2ndary sales transaction volume and price on a NFT
    # marketplace of your choice.
    con.close()
    print('\nGoodbye!')


if __name__ == "__main__":
    main()

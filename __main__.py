import argparse
import sqlite3
from src import penguin_data
from src.const import DEFAULT_SQL_LITE_DB
from src.const import DEFAULT_BATCH_SIZE

def main():
    parser = argparse.ArgumentParser(description="NFT Ranking")
    parser.add_argument('--refresh-penguin-data', action='store_true')
    parser.add_argument('--refresh-penguin-scores', action='store_true')
    parser.add_argument('--db-file', default=DEFAULT_SQL_LITE_DB)
    parser.add_argument('--batch-size', type=int, default=DEFAULT_BATCH_SIZE)
    args = parser.parse_args()

    con = sqlite3.connect(args.db_file)

    # Fetch data
    penguin_data.populate_penguin_data_table(con, args.batch_size, args.refresh_penguin_data)

    # Score them
    penguin_data.populate_penguin_score_table(con, args.batch_size, args.refresh_penguin_scores)

    # Calculate percentiles
    token_to_rarity_score_percentile = penguin_data.rarity_rank_and_percentiles(con)

    # TODO Remove eye-balling code
    from src.const import PENGUIN_SCORE_TABLE_NAME
    for row_data in con.execute(f'SELECT * FROM {PENGUIN_SCORE_TABLE_NAME} ORDER BY rarity_score DESC'):
        print(row_data, token_to_rarity_score_percentile[row_data[0]])
        print(penguin_data.rarity_rank_and_percentile_for_token(con, row_data[0]))
    # import pdb; pdb.set_trace()

    # Compare the scores against other tools  (ex: https://gem.xyz, https://raritysniper.com, …)

    # Bonus: analyze the correlation between rarity score and 2ndary sales transaction volume and price on a NFT
    # marketplace of your choice.
    con.close()


if __name__ == "__main__":
    main()

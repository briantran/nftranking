# NFT Ranking
NFT Ranking Exercise

Prompt: [Google Doc Link](https://docs.google.com/document/d/10B-xaHM0yeb9dt4DCVO193pJvbC18QbWpk6HgGIebl8/edit?usp=sharing).

## Instructions

1. Enter the repo directory.

```
cd nftranking/
```

2. Set up and activate virtual environment.

```
[[ -d env ]] || python3 -m venv env
source env/bin/activate
```

3. Install packages.

```
python3 -m pip install -r requirements.txt
```

4. Run the script from the virtual environment.

```
python3 runner.py
```

5. Once the data is ready. Input tokens you are curious about.

<img width="832" alt="Screen Shot 2022-06-07 at 10 31 55 AM" src="https://user-images.githubusercontent.com/409320/172446244-9a83017b-43c3-4474-b45f-9116c7289c76.png">


**NOTE:** This script by default uses the SQLite database stored at [penguin.db](penguin.db). This can be overridden via the `--db-file` flag.

```
# Override the file storing the database. If the database file does not exist
# or is not fully populated, the runner script will fully populate its tables.
python3 runner.py --db-file foo.db

# Use the in-memory database that only lasts until the session is over.
python3 runner.py --db-file :memory:
```

## Help Message

```
usage: runner.py [-h] [--refresh-penguin-data] [--refresh-penguin-scores] [--db-file DB_FILE] [--batch-size BATCH_SIZE]

NFT Ranking

optional arguments:
  -h, --help            show this help message and exit
  --refresh-penguin-data
                        Deletes already persisted penguin data (if it exists) and re-fetches.
  --refresh-penguin-scores
                        Deletes already persisted penguin scores (if it exists) and recalculates.
  --db-file DB_FILE     File path of SQLite DB. Use `:memory:` for in-memory store.
  --batch-size BATCH_SIZE
                        Max number of rows inserted into the database at a time.
```

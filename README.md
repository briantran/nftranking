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
python3 -m runner
```

**NOTE:** This script by default uses the SQLite database stored at `penguin.db`. This can be overridden via the `--db-file` flag.

```
# Override the file storing the database.
python3 -m runner --db-file foo.db

# Use the in-memory database that only lasts until the session is over.
python3 -m runner --db-file :memory:
```
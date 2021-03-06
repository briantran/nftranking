from pathlib import Path

DEFAULT_SQL_LITE_DB = str(Path(__file__).parent.parent / 'penguin.db')
PENGUIN_TABLE_NAME = 'penguins'
PENGUIN_SCORE_TABLE_NAME = 'penguin_scores'
PENGUIN_COLLECTION_SIZE = 8888
DEFAULT_BATCH_SIZE = 500
DATA_FETCH_SEMAPHORE_VALUE = 50

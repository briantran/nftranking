def row_count(connection, table):
    """Counts how many rows are within `table`.
    """
    return connection.execute(f'SELECT COUNT() FROM {table}').fetchone()[0]


def bulk_insert_statement(connection, insert_statement, insertion_values_list):
    """Inserts many rows into a table with a single insert statement.
    """
    if not insertion_values_list:
        return

    with connection:
        connection.executemany(insert_statement, insertion_values_list)


def chunks(iterable, n):
    """Utility method that breaks `iterable` into chunks of size `n`.
    """
    chunk_buffer = []
    for el in iterable:
        chunk_buffer.append(el)
        if len(chunk_buffer) > n:
            yield chunk_buffer
            chunk_buffer = []

    if chunk_buffer:
        yield chunk_buffer

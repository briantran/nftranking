def row_count(con, table):
    return con.execute(f'SELECT COUNT() FROM {table}').fetchone()[0]


def bulk_insert_statement(con, insert_statement, insertion_values_list):
    if not insertion_values_list:
        return

    with con:
        con.executemany(insert_statement, insertion_values_list)


def chunks(iterable, n):
    chunk_buffer = []
    for el in iterable:
        chunk_buffer.append(el)
        if len(chunk_buffer) > n:
            yield chunk_buffer
            chunk_buffer = []

    if chunk_buffer:
        yield chunk_buffer

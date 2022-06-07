from itertools import zip_longest


def row_count(con, table):
    return con.execute(f'SELECT COUNT() FROM {table}').fetchone()[0]


def flush_buffer(con, insert_statement, insert_buffer):
    if not insert_buffer:
        return

    with con:
        con.executemany(insert_statement, insert_buffer)

    insert_buffer.clear()


def chunks(iterable, n):
    args = [iter(iterable)] * n
    return zip_longest(*args)


def filtered_chunks(iterable, n):
    for chunk in chunks(iterable, n):
        yield list(x for x in chunk if x is not None)

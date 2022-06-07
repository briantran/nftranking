def row_count(con, table):
    return con.execute(f'SELECT COUNT() FROM {table}').fetchone()[0]


def flush_buffer(con, insert_statement, insert_buffer):
    if not insert_buffer:
        return

    with con:
        con.executemany(insert_statement, insert_buffer)

    insert_buffer.clear()



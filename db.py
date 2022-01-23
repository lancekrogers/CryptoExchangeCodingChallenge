import sqlite3
from sqlite3 import Error


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def bulk_insert_deposits(conn, values):
    """ bulk inserts rows into a table
    :param conn: Connection object
    :param values: list of tuples (rows) to be inserted
    :return:
    """
    try:
        conn.executemany("INSERT INTO deposits VALUES (?, ?, ?, ?)", values)
        conn.commit()
    except Error as e:
        print(e)


def deposit_summary_query(conn):
    """ Returns deposit summary data for the 7 known accounts
    :param conn: Connection object
    :return: cur.fetchall
    """
    try:
        curr = conn.cursor()
        curr.execute(""" SELECT name, COUNT(confirmations), SUM(amount)
    FROM deposits d LEFT OUTER JOIN accounts a on d.btc_address = a.btc_address
    WHERE confirmations > 5 GROUP BY name""")
        return curr.fetchall()
    except Error as e:
        print(e)
        return []


def max_min_deposit_query(conn):
    """ Returns largest valid deposit
    :param conn: Connection object
    :return: cur.fetchall
    """
    try:
        curr = conn.cursor()
        curr.execute("""SELECT MAX(amount), MIN(amount)
                     FROM deposits WHERE confirmations > 5;""")
        return curr.fetchone()
    except Error as e:
        print(e)
        return []


def initialize_database():
    database = r"accounts.db"

    sql_create_accounts_table = """ CREATE TABLE IF NOT EXISTS accounts (
                                        name CHAR(50) NOT NULL,
                                        btc_address CHAR(35) NOT NULL,
                                        UNIQUE (name, btc_address)
                                        ); """

    sql_create_deposits_table = """ CREATE TABLE IF NOT EXISTS deposits (
                                       btc_address CHAR(35),
                                       amount REAL,
                                       confirmations INTEGER,
                                       txid CHAR(63)
                                       );"""

    # create a database connection
    conn = create_connection(database)

    # create tables
    if conn is not None:
        create_table(conn, sql_create_accounts_table
                     )

        create_table(conn, sql_create_deposits_table
                     )
    else:
        print("Error! cannot create the database connection.")
        return

    # create accounts
    customers = [
        ("Wesley Crusher", "mvd6qFeVkqH6MNAS2Y2cLifbdaX5XUkbZJ"),
        ("Leonard McCoy", "mmFFG4jqAtw9MoCC88hw5FNfreQWuEHADp"),
        ("Jonathan Archer", "mzzg8fvHXydKs8j9D2a8t7KpSXpGgAnk4n"),
        ("Jadzia Dax", "2N1SP7r92ZZJvYKG2oNtzPwYnzw62up7mTo"),
        ("Montgomery Scott", "mutrAf4usv3HKNdpLwVD4ow2oLArL6Rez8"),
        ("James T. Kirk", "miTHhiX3iFhVnAEecLjybxvV5g8mKYTtnM"),
        ("Spock", "mvcyJMiAcSXKAEsQxbW9TYZ369rsMG6rVV")
    ]
    cur = conn.cursor()
    cur.execute("""select name, btc_address
                from accounts where name=? and btc_address=?""",
                customers[-1])
    result = cur.fetchone()
    if not result:
        try:
            for customer in customers:
                conn.execute(
                    f"insert into accounts(name, btc_address) values{customer}"
                )
                conn.commit()
        except Error as e:
            print(e)
    conn.close()


if __name__ == '__main__':
    initialize_database()

import json
import os
import sys
from db import (
    initialize_database, create_connection, bulk_insert_deposits,
    deposit_summary_query, max_min_deposit_query
)


def process_transaction_json(transactions):
    """
    returns tuple transaction data in the format required to be
        inserted into the deposits table and filtered for only deposits
    """
    return [tuple(transaction[key] for key in [
            'address', 'amount', 'confirmations', 'txid'
            ]) for transaction in transactions if transaction[
                    'amount'] > 0]


def add_deposits_to_db(conn, transactions):
    if conn is not None:
        deposit_transaction_values = process_transaction_json(transactions)
        bulk_insert_deposits(conn, deposit_transaction_values)
    else:
        print("Error! cannot create the database connection.")


def deposit_summary(conn):
    query_dict = {row[0]: row[1:] for row in deposit_summary_query(conn)}
    max_min = max_min_deposit_query(conn)
    return [
        f"""Deposited for Wesley Crusher: count={
            query_dict['Wesley Crusher'][0]} sum={
                query_dict['Wesley Crusher'][1]:.8f}""",
        f"""Deposited for Leonard McCoy: count={
            query_dict['Leonard McCoy'][0]} sum={
                query_dict['Leonard McCoy'][1]:.8f}""",
        f"""Deposited for Jonathan Archer: count={
            query_dict[ 'Jonathan Archer'][0]} sum={
                query_dict['Jonathan Archer'][1]:.8f}""",
        f"""Deposited for Jadzia Dax: count={
            query_dict[ 'Jadzia Dax'][0]} sum={
            query_dict['Jadzia Dax'][1]:.8f}""",
        f"""Deposited for Montgomery Scott: count={
            query_dict[ 'Montgomery Scott'][0]} sum={
                query_dict['Montgomery Scott'][1]:.8f}""",
        f"""Deposited for James T. Kirk: count={
            query_dict[ 'James T. Kirk'][0]} sum={
            query_dict['James T. Kirk'][1]:.8f}""",
        f"""Deposited for Spock: count={
            query_dict[ 'Spock'][0]} sum={query_dict['Spock'][1]:.8f}""",
        f"""Deposited without reference: count={
            query_dict[None][0]} sum={query_dict[None][1]:.8f}""",
        f"Smallest valid deposit: {max_min[1]:.8f}",
        f"Largest valid deposit: {max_min[0]:.8f}"
    ]


def main():
    # Delete database if it exists
    if os.path.exists('accounts.db'):
        os.remove('accounts.db')

    initialize_database()

    # Read json files to memory
    with open('transactions-1.json', 'r') as file:
        transactions_file1 = json.loads(file.read())

    with open('transactions-2.json', 'r') as file:
        transactions_file2 = json.loads(file.read())

    # connect to database
    conn = create_connection(r'accounts.db')

    # Add transactions-1.json deposits to the deposits table
    add_deposits_to_db(conn, transactions_file1['transactions'])

    # Add transactions-2.json deposits to the deposits table
    add_deposits_to_db(conn, transactions_file2['transactions'])

    for s in deposit_summary(conn):
        sys.stdout.write(s + "\n")


if __name__ == '__main__':
    main()

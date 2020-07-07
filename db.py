import sys
import json
from pprint import pprint
from datetime import datetime

from sqlalchemy.sql import text
from sqlalchemy import create_engine


def get_db_engine(echo=False):
    with open("dbconfig.json", "r") as f:
        dbconfig = json.load(f)
        host, database = dbconfig["host"], dbconfig["database"]
        username, password = dbconfig["username"], dbconfig["password"]

        engine = create_engine(
            f"mysql+pymysql://{username}:{password}@{host}/{database}",
            echo=echo, pool_size=6, max_overflow=10, encoding='latin1'
        )
        return engine


def test_select_query(conn):
    result = conn.execute("""
    SELECT * from unibet_football_main LIMIT 2;
    """)
    data = []
    for row in result:
        data.append(dict(zip(result.keys(), row)))
    pprint(data)


def test_insert_query(conn):
    data = {
        'match': "Test match - Testing purpose",
        'team1': 'Test match',
        'team2': 'Testing purpose',
        'quote_team1': 11.11,
        'quote_draw': 11.11,
        'quote_team2': 11.11,
        'quote_for_team1': 11
    }
    query = text("""
    INSERT INTO unibet_football_main(gameMatch, team1, team2, quoteTeam1, quoteDraw, quoteTeam2, quoteForT1) 
    VALUES(:match, :team1, :team2, :quote_team1, :quote_draw, :quote_team2, :quote_for_team1);
    """)
    result = conn.execute(query, **data)
    print("result: ", result)


if __name__ == "__main__":
    if "--test-db" in sys.argv:
        engine = get_db_engine()
        with engine.connect() as conn:
            test_select_query(conn)
            test_insert_query(conn)
    else:
        engine = get_db_engine(echo=True)
        engine.connect()

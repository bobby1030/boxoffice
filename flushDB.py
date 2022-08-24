"""
flushDB.py:
    - flush the database
    - re-fetch all historical box office data since 2020-03-01 (earliest available data according to https://data.gov.tw/dataset/94224)
"""

import sqlite3
import asyncio

from app.crawler.crawl import BoxOfficesRange, fetchBoxOffices


if __name__ == "__main__":

    db = sqlite3.connect("boxoffice.db")
    dbcursor = db.cursor()

    # Drop pre-existing table
    dbcursor.execute("DROP TABLE IF EXISTS BoxOfficeDaily")
    # Initialize database
    dbcursor.execute(
        """
        CREATE TABLE BoxOfficeDaily (
            date, country, name, releaseDate, issue, produce, theaterCount, tickets, ticketChangeRate, amounts, totalTickets, totalAmounts,
            ongoingDays INT GENERATED ALWAYS AS (julianday(date)-julianday(releaseDate)) NOT NULL,
            PRIMARY KEY (name, date)
        ) 
        """
    )

    boxOfficeDailys = BoxOfficesRange(start="2020-03-01")
    asyncio.run(
        fetchBoxOffices(boxOfficeDailys, concurrency=10, persist=True, dbconnection=db)
    )

    db.close()

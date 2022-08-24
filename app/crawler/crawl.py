import datetime
import numpy as np
import asyncio
import sqlite3

from app.crawler.BoxOfficeDaily import BoxOfficeDaily


def BoxOfficesRange(
    # Default to init today's box office
    start: str | datetime.date = datetime.date.today(),
    end: str | datetime.date = datetime.date.today(),
) -> list[BoxOfficeDaily]:
    """
    Create list of BoxOfficeDaily objects for a range of dates

    @input: start and end date of box office data
    @output: list of BoxOfficeDaily objects
    """
    daterange = np.arange(np.datetime64(start), np.datetime64(end) + 1)
    boxOffices = [BoxOfficeDaily(date) for date in daterange]
    return boxOffices


def insertBoxOfficeRows(boxOfficeDaily, db):
    """
    Write box office data to database

    @input: BoxOfficeDaily object, database connection
    @output: None
    """
    try:
        dbcursor = db.cursor()
    except Exception as e:
        print("Failed to get DB cursor")
        print(e)

    if boxOfficeDaily.response != None:
        print(f"Writing: {boxOfficeDaily.dateStr}")
        for row in boxOfficeDaily.response:
            dbcursor.execute(
                """
                INSERT INTO BoxOfficeDaily
                (date, country, name, releaseDate, issue, produce, theaterCount, tickets, ticketChangeRate, amounts, totalTickets, totalAmounts)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    boxOfficeDaily.dateStr,
                    row["country"],
                    row["name"],
                    row["releaseDate"],
                    row["issue"],
                    row["produce"],
                    row["theaterCount"],
                    row["tickets"],
                    row["ticketChangeRate"],
                    row["amounts"],
                    row["totalTickets"],
                    row["totalAmounts"],
                ),
            )
        db.commit()
    else:
        print(f"Nothing to insert for {boxOfficeDaily.dateStr}")


async def fetchBoxOffices(
    boxOffices, concurrency=5, persist=False, dbconnection: sqlite3.Connection = None
):
    """
    Pull box office data associated with BoxOfficeDaily objects from remote API

    @input: list of BoxOfficeDaily objects, concurrency, persist, database connection
    @output: None
    """
    semaphore = asyncio.Semaphore(concurrency)

    async def fetchWorker(boxOfficeDaily):
        async with semaphore:
            await boxOfficeDaily.getData()

            if persist == True:
                insertBoxOfficeRows(boxOfficeDaily, dbconnection)
            else:
                pass

    BoxTasks = [fetchWorker(box) for box in boxOffices]

    # Wait for all tasks to complete
    await asyncio.gather(*BoxTasks)

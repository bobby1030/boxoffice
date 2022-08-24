import asyncio
import aiohttp
import datetime
import numpy


class BoxOfficeDaily:
    def __init__(self, date: datetime.date = datetime.date.today()):

        # Convert numpy's datetime64 to datetime.date
        if type(date) == numpy.datetime64:
            date = date.astype(datetime.date)
        elif type(date) == str:
            date = datetime.datetime.strptime(date, "%Y-%m-%d").date()

        self.date = date
        self.dateStr = date.strftime("%Y-%m-%d")
        self.weekday = date.strftime("%a")
        self.response = None

    async def getData(self) -> list:

        url = "https://boxoffice.tfi.org.tw/api/export"
        params = {
            "start": self.dateStr,
            "end": self.dateStr,
        }

        if self.response == None:
            print(f"Fetching box office: {self.dateStr}")

            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, params=params) as response:
                        res = await response.json()

                        start = datetime.datetime.fromisoformat(res["start"])
                        end = datetime.datetime.fromisoformat(res["end"])

                        if start != end:
                            raise Exception(
                                f"Start date and end date are not equal: {start} != {end}"
                            )
                        if len(res["list"]) == 0:
                            raise Exception(f"No data found for {self.dateStr}")

                        # Save returned box office list
                        self.response = res["list"]

            except Exception as e:
                print(f"Failed to fetch box office: {self.dateStr}")
                print(e)
                return None
        else:
            print(f"Already fetched box office: {self.dateStr}. Skpping.")
        return self.response

    def __repr__(self) -> str:
        return f"<BoxOfficeDaily {self.dateStr} ({self.weekday})>"

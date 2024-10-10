import asyncio
import datetime
import os
import sqlite3
from datetime import timedelta
from TikTokApi import TikTokApi

max_fetch = 1

ms_token = os.environ.get("ms_token", None)


async def crawl_videos():
    global max_fetch

    async with TikTokApi() as api:
        await api.create_sessions(ms_tokens=[ms_token], num_sessions=1, sleep_after=3, browser="webkit")
        async for video in api.trending.videos(count=max_fetch):
            async for related_video in video.related_videos(count=max_fetch):
                video_id = int(related_video.id)
                likes = int(related_video.stats['diggCount'])

                today = datetime.datetime.now()
                date = related_video.create_time
                if today - date < timedelta(days=30):
                    await insert_tiktok_video(video_id, likes, date)


def create_tables():
    sql_statements = [
        """CREATE TABLE IF NOT EXISTS tiktok_videos (
                id INTEGER PRIMARY KEY, 
                likes INTEGER NOT NULL, 
                create_date TEXT
        );"""
    ]

    try:
        with sqlite3.connect("viewsort.db") as conn:
            cursor = conn.cursor()
            for statement in sql_statements:
                cursor.execute(statement)
    except sqlite3.Error as e:
        print(e)


async def insert_tiktok_video(video_id: int, likes: int, create_date: datetime.datetime):
    try:
        with sqlite3.connect("viewsort.db") as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO tiktok_videos(id, likes, create_date) VALUES(?,?,?)"
                           "ON CONFLICT(id)"
                           "DO UPDATE SET ",
                           (video_id, likes, str(create_date)))
            conn.commit()
    except sqlite3.Error as e:
        print(e)


if __name__ == "__main__":
    create_tables()
    asyncio.run(crawl_videos())

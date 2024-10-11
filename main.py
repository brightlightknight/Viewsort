import asyncio
import datetime
import logging
import os
import sqlite3
from datetime import timedelta
from TikTokApi import TikTokApi
from TikTokApi.api.video import Video

ms_token = os.environ.get("ms_token", None)

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


# noinspection PyTypeChecker
async def crawl_videos(videos: list[Video], video_fetch=30, depth=0, max_depth=4):
    logging.info(f"Crawling... depth: {depth}.")
    for video in videos:
        async for related_video in video.related_videos(count=video_fetch):
            video_id = int(related_video.id)
            likes = int(related_video.stats['diggCount'])

            today = datetime.datetime.now()
            date = related_video.create_time

            # Here we check if the video isn't too old. If it is, ignore it for the crawl.
            if today - date < timedelta(days=30):
                await insert_tiktok_video(video_id, likes, date)
                if depth + 1 < max_depth:
                    related_videos = []
                    async for subrelated_video in related_video.related_videos(count=video_fetch):
                        related_videos.append(subrelated_video)
                    await crawl_videos(videos=related_videos, video_fetch=video_fetch, depth=depth + 1)


async def create_tables():
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
            cursor.execute("""INSERT INTO tiktok_videos(id, likes, create_date) VALUES(?,?,?)
                           ON CONFLICT(id) DO UPDATE SET
                           likes=excluded.likes, 
                           create_date=excluded.create_date;""",
                           (video_id, likes, str(create_date)))
            conn.commit()
    except sqlite3.Error as e:
        print(e)


async def main():
    logging.info("Initializing...")
    await create_tables()
    logging.info("Created tables")
    async with TikTokApi() as api:
        await api.create_sessions(ms_tokens=[ms_token], num_sessions=1, sleep_after=3, browser="webkit")
        logging.info("Started new user session")
        logging.info("Loading initial videos from trending...")
        init_videos = []
        async for video in api.trending.videos(count=30):
            init_videos.append(video)
        logging.info("Got initial videos to crawl")
        await crawl_videos(videos=init_videos, video_fetch=1, max_depth=2)
        logging.info("Done crawling!")


if __name__ == "__main__":
    asyncio.run(main())

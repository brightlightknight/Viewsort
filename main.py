import asyncio
import datetime as dt
import logging
import multiprocessing
import os
import sqlite3
import threading
import time
from datetime import timedelta
from sqlite3 import Connection

from TikTokApi.exceptions import InvalidResponseException
from proxygetter import ProxyManager

from TikTokApi import TikTokApi
from TikTokApi.api.video import Video

MAX_VIDEO_AGE_HOURS = 720
UPDATE_RATE_HOURS = 8

ms_token = os.environ.get("ms_token", None)

manager = ProxyManager()
logger = logging.getLogger("TiktokViewer")

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


class DatabaseTables:
    @staticmethod
    async def get_connection() -> Connection:
        try:
            with sqlite3.connect("viewsort.db") as conn:
                return conn
        except sqlite3.Error as e:
            print(e)

    async def create_tables(self) -> None:
        sql_statements = [
            """CREATE TABLE IF NOT EXISTS tiktok_videos (
                    id INTEGER PRIMARY KEY,
                    views INTEGER NOT NULL,
                    likes INTEGER NOT NULL, 
                    create_date INTEGER NOT NULL,
                    update_date INTEGER NOT NULL,
                    url TEXT NOT NULL
            );"""
        ]
        conn = await self.get_connection()

        cursor = conn.cursor()
        for statement in sql_statements:
            cursor.execute(statement)

    async def get_tiktok_total_video_count(self) -> int:
        conn = await self.get_connection()
        cursor = conn.cursor()
        cursor.execute(""" SELECT COUNT(id) FROM tiktok_videos; """)
        return cursor.fetchone()[0]

    async def get_tiktok_top_liked_videos(self, limit: int = 50, offset: int = 0):
        conn = await self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""  SELECT id, views, likes, create_date, update_date, url FROM tiktok_videos
                            ORDER BY likes DESC
                            LIMIT ? OFFSET ?;""",
                       (limit, offset))
        fetch = cursor.fetchall()
        data = []
        for d in fetch:
            data.append(
                {'id': d[0], 'views': d[1], 'likes': d[2], 'create_date': d[3], 'update_date': d[4], 'url': d[5]})
        return data

    async def get_tiktok_random_videos(self, limit: int = 50):
        conn = await self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""  SELECT id, views, likes, create_date, update_date, url FROM tiktok_videos
                            ORDER BY RANDOM() LIMIT ?;""",
                       (limit,))
        fetch = cursor.fetchall()
        data = []
        for d in fetch:
            data.append(
                {'id': d[0], 'views': d[1], 'likes': d[2], 'create_date': d[3], 'update_date': d[4], 'url': d[5]})
        return data

    async def get_outdated_videos(self):
        epoch = int(time.time())
        conn = await self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""  SELECT id, views, likes, create_date, update_date, url FROM tiktok_videos
                            WHERE update_date < ? - ?
                            ORDER BY update_date ASC;""",
                       (epoch, MAX_VIDEO_AGE_HOURS * 60 * 60))
        fetch = cursor.fetchall()
        data = []
        for d in fetch:
            data.append(
                {'id': d[0], 'views': d[1], 'likes': d[2], 'create_date': d[3], 'update_date': d[4], 'url': d[5]})
        return data

    async def get_tiktok_video(self, video_id: int) -> dict | None:
        conn = await self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""SELECT id, views, likes, create_date, update_date, url FROM tiktok_videos WHERE id=?;""",
                       (video_id,))
        data = cursor.fetchone()
        if data is None:
            return None
        return {'id': data[0], 'views': data[1], 'likes': data[2], 'create_date': data[3], 'update_date': data[4],
                'url': data[5]}

    async def insert_tiktok_video(self, video_id: int, views: int, likes: int,
                                  create_date: dt.datetime, update_date: dt.datetime, url: str):
        conn = await self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""INSERT INTO tiktok_videos(id, views, likes, create_date, update_date, url) VALUES(?,?,?,?,?,?)
                        ON CONFLICT(id) DO UPDATE SET
                        views=excluded.views,
                        likes=excluded.likes, 
                        create_date=excluded.create_date,
                        update_date=excluded.update_date;""",
                       (video_id, views, likes, int(create_date.timestamp()), int(update_date.timestamp()), url))
        conn.commit()

    async def remove_old_tiktok_videos(self):
        epoch = int(time.time())
        conn = await self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""DELETE FROM tiktok_videos WHERE create_date < ? - ?""",
                       (epoch, MAX_VIDEO_AGE_HOURS * 60 * 60))
        conn.commit()
        return cursor.rowcount

    async def remove_tiktok_video(self, video_id: int):
        conn = await self.get_connection()
        cursor = conn.cursor()
        cursor.execute("""DELETE FROM tiktok_videos WHERE id = ?""",
                       (video_id,))
        conn.commit()
        return cursor.rowcount


class TikTokCrawler:
    database_tables: DatabaseTables

    videos_total = 0
    videos_added = 0
    videos_updated = 0

    # noinspection PyTypeChecker
    async def crawl_videos(self, videos: list[Video], max_depth=100, depth=0):
        for video in videos:
            related_videos = []
            async for related_video in video.related_videos():
                related_videos.append(related_video)

            for related_video in related_videos:
                video_id = int(related_video.id)
                stored_video = await self.database_tables.get_tiktok_video(video_id)

                # Don't update if it doesn't need to
                if stored_video is not None and (
                        time.time() - stored_video['update_date'] < UPDATE_RATE_HOURS * 60 * 60):
                    continue

                likes = int(related_video.stats['diggCount'])
                views = int(related_video.stats['playCount'])
                url = related_video.url
                if url is None:
                    user = related_video.author.username
                    url = f"https://www.tiktok.com/@{user}/video/{video_id}"

                today = dt.datetime.now()
                date = related_video.create_time

                # Here we check if the video isn't too old. If it is, ignore it for the crawl.
                if today - date < timedelta(hours=MAX_VIDEO_AGE_HOURS):
                    if await self.database_tables.get_tiktok_video(video_id):
                        self.videos_updated += 1
                    else:
                        self.videos_added += 1

                    logger.info(f"Tiktok Videos [ Total: {self.videos_total + self.videos_added},"
                                f" Added: {self.videos_added}, Updated: {self.videos_updated} ]")

                    await self.database_tables.insert_tiktok_video(video_id, views, likes, date, today, url)
                    if stored_video is not None and depth + 1 < max_depth:
                        related_videos = []
                        async for subrelated_video in related_video.related_videos():
                            related_videos.append(subrelated_video)
                        await self.crawl_videos(videos=related_videos, depth=depth + 1, max_depth=max_depth)


async def get_tiktok_video_from_dict(api, dbt, video_dict):
    try:
        video = api.video(data=await api.video(url=video_dict['url']).info())
    except InvalidResponseException:
        await dbt.remove_tiktok_video(video_dict['id'])
        logger.info(f"Deleting invalid video")
        return None
    if video.stats is None:
        await dbt.remove_tiktok_video(video_dict['id'])
        logger.info(f"Deleting invalid video")
        return None
    return video


async def updater_thread(api, dbt):
    while True:
        removed_video_count = await dbt.remove_old_tiktok_videos()
        if removed_video_count > 0:
            logger.info(f"Removed {removed_video_count} old videos (>30 days old)")

        outdated_videos = await dbt.get_outdated_videos()
        if len(outdated_videos) > 0:
            logger.info("Updating old videos...")
            for i in range(len(outdated_videos)):
                video_dict = outdated_videos[i]
                video = await get_tiktok_video_from_dict(api, dbt, video_dict)
                if video is None:
                    continue

                likes = int(video.stats['diggCount'])
                views = int(video.stats['playCount'])
                today = dt.datetime.now()
                date = video.create_time

                await dbt.insert_tiktok_video(video.id, views, likes, date, today, video_dict['url'])
                logger.info(f"Updated video {video.id} ({i + 1} / {len(outdated_videos)})")
            logger.info("Done updating, checking in 60 seconds...")
        time.sleep(60)


async def main():
    logger.info("Initializing...")
    dbt = DatabaseTables()
    await dbt.create_tables()
    logger.info("Created tables")

    proxies = []
    """
    // Note: This isn't working at the moment. Any and all proxies refuse to connect to tiktok.
    
    logger.info("Getting proxies...")
    proxies_raw = manager.get_proxies(https=True, google=True, last_checked_max=600,
                                      filter_validity_url="www.tiktok.com")
    for proxy in proxies_raw:
        proxies.append({'server': f"{proxy.ip}:{proxy.port}"})
        logger.info(f">> Using proxy with ip {proxy.ip}:{proxy.port}")
    logger.info("Got all proxies to scrap")
    """

    async with TikTokApi() as api:
        await api.create_sessions(ms_tokens=[ms_token], num_sessions=5, proxies=proxies,
                                  sleep_after=3, browser="webkit", suppress_resource_load_types=['image', 'media'])

        logger.info("Started new user sessions")

        updater = threading.Thread(target=asyncio.run, args=(updater_thread(api, dbt),))
        updater.start()

        crawl_cycle = 0
        while True:
            crawl_cycle += 1
            init_videos = []
            total_videos = await dbt.get_tiktok_total_video_count()
            if total_videos > 100:
                logger.info("Loading initial videos from database...")
                rand_videos = await dbt.get_tiktok_random_videos(limit=30)
                for video_dict in rand_videos:
                    video = await get_tiktok_video_from_dict(api, dbt, video_dict)
                    if video is None:
                        continue
                    init_videos.append(video)
            else:
                logger.info("Loading initial videos from trending...")
                async for video in api.trending.videos(count=30):
                    init_videos.append(video)

            logger.info("Got initial videos to crawl")

            # Starting the crawler
            crawler = TikTokCrawler()
            crawler.database_tables = dbt
            crawler.videos_total = await dbt.get_tiktok_total_video_count()

            # Dividing initial video task into coroutines
            cpu_count = multiprocessing.cpu_count()
            n = int(len(init_videos) / cpu_count)
            init_video_chunks = [init_videos[i * n:(i + 1) * n] for i in range((len(init_videos) + n - 1) // n)]

            coroutines = [crawler.crawl_videos(init_video_chunks[i]) for i in range(len(init_video_chunks))]

            await asyncio.gather(*coroutines)
            logger.info(f"Completed crawl cycle {crawl_cycle}")


if __name__ == "__main__":
    asyncio.run(main())

from __future__ import annotations
from .sessions import RandomUserAgentSession
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random
import logging
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

logging.basicConfig(
    filename="YARS.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


class YARS:
    __slots__ = ("headers", "session", "proxy", "timeout")

    def __init__(self, proxy=None, timeout=10, random_user_agent=True):
        self.session = RandomUserAgentSession() if random_user_agent else requests.Session()
        self.proxy = proxy
        self.timeout = timeout

        retries = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
        )

        self.session.mount("https://", HTTPAdapter(max_retries=retries))

        if proxy:
            self.session.proxies.update({"http": proxy, "https": proxy})

    def handle_search(
        self,
        url,
        params,
        after=None,
        before=None,
        since_utc=None,
        until_utc=None,
        pages=1,
        min_score=0,
        min_comments=0,
    ):
        if after:
            params["after"] = after
        if before:
            params["before"] = before

        results = []
        seen_ids = set()

        for page in range(pages):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
                response.raise_for_status()
                logging.info("Search request successful (page %d)", page + 1)
            except Exception as e:
                logging.info("Search request unsuccessful due to: %s", e)
                print(f"Failed to fetch search results: {e}")
                break

            data = response.json()
            posts = data["data"]["children"]
            if not posts:
                break

            for post in posts:
                post_data = post["data"]
                post_id = post_data.get("id", "")
                if post_id in seen_ids:
                    continue
                seen_ids.add(post_id)

                created_utc = post_data.get("created_utc", 0)
                if since_utc and created_utc < since_utc:
                    continue
                if until_utc and created_utc > until_utc:
                    continue

                score = post_data.get("score", 0)
                if score < min_score:
                    continue

                num_comments = post_data.get("num_comments", 0)
                if num_comments < min_comments:
                    continue

                results.append(
                    {
                        "post_id": post_id,
                        "title": post_data["title"],
                        "link": f"https://www.reddit.com{post_data['permalink']}",
                        "description": post_data.get("selftext", "")[:269],
                        "author": post_data.get("author", ""),
                        "score": score,
                        "num_comments": num_comments,
                        "created_utc": created_utc,
                        "subreddit": post_data.get("subreddit", ""),
                    }
                )

            next_after = data["data"].get("after")
            if not next_after:
                break
            params["after"] = next_after
            time.sleep(random.uniform(1, 2))

        logging.info("Search returned %d results", len(results))
        return results

    def search_reddit(
        self,
        query,
        limit=25,
        after=None,
        before=None,
        time_filter="all",
        since_utc=None,
        until_utc=None,
        pages=1,
        min_score=0,
        min_comments=0,
    ):
        url = "https://www.reddit.com/search.json"
        params = {"q": query, "limit": limit, "sort": "relevance", "type": "link", "t": time_filter}
        return self.handle_search(url, params, after, before, since_utc, until_utc, pages, min_score, min_comments)

    def search_subreddit(
        self,
        subreddit,
        query,
        limit=25,
        after=None,
        before=None,
        sort="relevance",
        time_filter="all",
        since_utc=None,
        until_utc=None,
        pages=1,
        min_score=0,
        min_comments=0,
    ):
        url = f"https://www.reddit.com/r/{subreddit}/search.json"
        params = {"q": query, "limit": limit, "sort": sort, "type": "link", "restrict_sr": "on", "t": time_filter}
        return self.handle_search(url, params, after, before, since_utc, until_utc, pages, min_score, min_comments)

    def search_subreddits(
        self,
        subreddits,
        query,
        limit=25,
        sort="relevance",
        time_filter="all",
        since_utc=None,
        until_utc=None,
        pages=3,
        min_score=0,
        min_comments=0,
        workers=5,
    ):
        """Search multiple subreddits in parallel and return deduplicated results."""

        def search_one(subreddit):
            return self.search_subreddit(
                subreddit,
                query,
                limit=limit,
                sort=sort,
                time_filter=time_filter,
                since_utc=since_utc,
                until_utc=until_utc,
                pages=pages,
                min_score=min_score,
                min_comments=min_comments,
            )

        all_results = []
        seen_ids = set()

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(search_one, sr): sr for sr in subreddits}
            for future in as_completed(futures):
                sr = futures[future]
                try:
                    results = future.result()
                    added = 0
                    for r in results:
                        post_id = r.get("post_id", r["link"])
                        if post_id not in seen_ids:
                            seen_ids.add(post_id)
                            all_results.append(r)
                            added += 1
                    logging.info("r/%s: %d results", sr, added)
                    print(f"r/{sr}: {added} results")
                except Exception as e:
                    logging.info("Failed search for r/%s: %s", sr, e)
                    print(f"r/{sr}: failed — {e}")

        return all_results

    def handle_comment_search(
        self,
        url,
        params,
        after=None,
        since_utc=None,
        until_utc=None,
        pages=1,
        min_score=0,
    ):
        if after:
            params["after"] = after

        results = []
        seen_ids = set()

        for page in range(pages):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
                response.raise_for_status()
                logging.info("Comment search request successful (page %d)", page + 1)
            except Exception as e:
                logging.info("Comment search request unsuccessful: %s", e)
                print(f"Failed to fetch comment search results: {e}")
                break

            data = response.json()
            children = data["data"]["children"]
            if not children:
                break

            for item in children:
                if item.get("kind") != "t1":
                    continue
                comment_data = item["data"]
                comment_id = comment_data.get("id", "")
                if comment_id in seen_ids:
                    continue
                seen_ids.add(comment_id)

                created_utc = comment_data.get("created_utc", 0)
                if since_utc and created_utc < since_utc:
                    continue
                if until_utc and created_utc > until_utc:
                    continue

                score = comment_data.get("score", 0)
                if score < min_score:
                    continue

                results.append(
                    {
                        "comment_id": comment_id,
                        "body": comment_data.get("body", ""),
                        "author": comment_data.get("author", ""),
                        "score": score,
                        "created_utc": created_utc,
                        "subreddit": comment_data.get("subreddit", ""),
                        "post_title": comment_data.get("link_title", ""),
                        "post_link": f"https://www.reddit.com{comment_data.get('link_permalink', '')}",
                        "comment_link": f"https://www.reddit.com{comment_data.get('permalink', '')}",
                    }
                )

            next_after = data["data"].get("after")
            if not next_after:
                break
            params["after"] = next_after
            time.sleep(random.uniform(1, 2))

        logging.info("Comment search returned %d results", len(results))
        return results

    def search_comments(
        self,
        query,
        limit=25,
        after=None,
        time_filter="all",
        since_utc=None,
        until_utc=None,
        pages=1,
        min_score=0,
    ):
        url = "https://www.reddit.com/search.json"
        params = {"q": query, "limit": limit, "sort": "relevance", "type": "comment", "t": time_filter}
        return self.handle_comment_search(url, params, after, since_utc, until_utc, pages, min_score)

    def search_subreddit_comments(
        self,
        subreddit,
        query,
        limit=25,
        after=None,
        sort="relevance",
        time_filter="all",
        since_utc=None,
        until_utc=None,
        pages=1,
        min_score=0,
    ):
        url = f"https://www.reddit.com/r/{subreddit}/search.json"
        params = {"q": query, "limit": limit, "sort": sort, "type": "comment", "restrict_sr": "on", "t": time_filter}
        return self.handle_comment_search(url, params, after, since_utc, until_utc, pages, min_score)

    def search_subreddits_comments(
        self,
        subreddits,
        query,
        limit=25,
        sort="relevance",
        time_filter="all",
        since_utc=None,
        until_utc=None,
        pages=3,
        min_score=0,
        workers=5,
    ):
        """Search comments across multiple subreddits in parallel and return deduplicated results."""

        def search_one(subreddit):
            return self.search_subreddit_comments(
                subreddit,
                query,
                limit=limit,
                sort=sort,
                time_filter=time_filter,
                since_utc=since_utc,
                until_utc=until_utc,
                pages=pages,
                min_score=min_score,
            )

        all_results = []
        seen_ids = set()

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(search_one, sr): sr for sr in subreddits}
            for future in as_completed(futures):
                sr = futures[future]
                try:
                    results = future.result()
                    added = 0
                    for r in results:
                        comment_id = r.get("comment_id", r["comment_link"])
                        if comment_id not in seen_ids:
                            seen_ids.add(comment_id)
                            all_results.append(r)
                            added += 1
                    logging.info("r/%s comments: %d results", sr, added)
                    print(f"r/{sr}: {added} comments")
                except Exception as e:
                    logging.info("Failed comment search for r/%s: %s", sr, e)
                    print(f"r/{sr}: failed — {e}")

        return all_results

    def scrape_post_details(self, permalink, max_comments=None, min_comment_score=None, max_depth=None):
        url = f"https://www.reddit.com{permalink}.json"

        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            logging.info("Post details request successful: %s", url)
        except Exception as e:
            logging.info("Post details request unsuccessful: %s", e)
            print(f"Failed to fetch post data: {e}")
            return None

        post_data = response.json()
        if not isinstance(post_data, list) or len(post_data) < 2:
            logging.info("Unexpected post data structure")
            print("Unexpected post data structure")
            return None

        main_post = post_data[0]["data"]["children"][0]["data"]
        title = main_post["title"]
        body = main_post.get("selftext", "")

        comments = self._extract_comments(
            post_data[1]["data"]["children"],
            max_depth=max_depth,
            min_score=min_comment_score,
        )

        if max_comments is not None:
            comments = sorted(
                comments,
                key=lambda c: c["score"] if isinstance(c["score"], int) else 0,
                reverse=True,
            )[:max_comments]

        logging.info("Successfully scraped post: %s", title)
        return {"title": title, "body": body, "comments": comments}

    def _extract_comments(self, comments, max_depth=None, min_score=None, _depth=0):
        extracted_comments = []
        for comment in comments:
            if isinstance(comment, dict) and comment.get("kind") == "t1":
                comment_data = comment.get("data", {})
                score = comment_data.get("score", 0)
                if min_score is not None and isinstance(score, int) and score < min_score:
                    continue
                extracted_comment = {
                    "author": comment_data.get("author", ""),
                    "body": comment_data.get("body", ""),
                    "score": score,
                    "replies": [],
                }
                if max_depth is None or _depth < max_depth:
                    replies = comment_data.get("replies", "")
                    if isinstance(replies, dict):
                        extracted_comment["replies"] = self._extract_comments(
                            replies.get("data", {}).get("children", []),
                            max_depth=max_depth,
                            min_score=min_score,
                            _depth=_depth + 1,
                        )
                extracted_comments.append(extracted_comment)
        return extracted_comments

    def scrape_user_data(self, username, limit=10):
        logging.info("Scraping user data for %s, limit: %d", username, limit)
        base_url = f"https://www.reddit.com/user/{username}/.json"
        params = {"limit": min(100, limit), "after": None}
        all_items = []
        count = 0

        while count < limit:
            try:
                response = self.session.get(base_url, params=params, timeout=self.timeout)
                response.raise_for_status()
                logging.info("User data request successful")
            except Exception as e:
                logging.info("User data request unsuccessful: %s", e)
                print(f"Failed to fetch data for user {username}: {e}")
                break

            try:
                data = response.json()
            except ValueError:
                print(f"Failed to parse JSON response for user {username}.")
                break

            if "data" not in data or "children" not in data["data"]:
                print(f"No 'data' or 'children' field found in response for user {username}.")
                logging.info("No 'data' or 'children' field found in response")
                break

            items = data["data"]["children"]
            if not items:
                logging.info("No more items found for user %s", username)
                break

            for item in items:
                kind = item["kind"]
                item_data = item["data"]
                if kind == "t3":
                    all_items.append(
                        {
                            "type": "post",
                            "title": item_data.get("title", ""),
                            "subreddit": item_data.get("subreddit", ""),
                            "url": f"https://www.reddit.com{item_data.get('permalink', '')}",
                            "created_utc": item_data.get("created_utc", ""),
                        }
                    )
                elif kind == "t1":
                    all_items.append(
                        {
                            "type": "comment",
                            "subreddit": item_data.get("subreddit", ""),
                            "body": item_data.get("body", ""),
                            "created_utc": item_data.get("created_utc", ""),
                            "url": f"https://www.reddit.com{item_data.get('permalink', '')}",
                        }
                    )
                count += 1
                if count >= limit:
                    break

            params["after"] = data["data"].get("after")
            if not params["after"]:
                break

            time.sleep(random.uniform(1, 2))
            logging.info("Sleeping for random time")

        logging.info("Successfully scraped user data for %s", username)
        return all_items

    def fetch_subreddit_posts(self, subreddit, limit=10, category="hot", time_filter="all"):
        logging.info(
            "Fetching subreddit/user posts for %s, limit: %d, category: %s, time_filter: %s",
            subreddit, limit, category, time_filter,
        )
        if category not in ["hot", "top", "new", "userhot", "usertop", "usernew"]:
            raise ValueError(
                "Category for Subreddit must be either 'hot', 'top', or 'new' "
                "or for User must be 'userhot', 'usertop', or 'usernew'"
            )

        batch_size = min(100, limit)
        total_fetched = 0
        after = None
        all_posts = []

        while total_fetched < limit:
            if category == "hot":
                url = f"https://www.reddit.com/r/{subreddit}/hot.json"
            elif category == "top":
                url = f"https://www.reddit.com/r/{subreddit}/top.json"
            elif category == "new":
                url = f"https://www.reddit.com/r/{subreddit}/new.json"
            elif category == "userhot":
                url = f"https://www.reddit.com/user/{subreddit}/submitted/hot.json"
            elif category == "usertop":
                url = f"https://www.reddit.com/user/{subreddit}/submitted/top.json"
            else:
                url = f"https://www.reddit.com/user/{subreddit}/submitted/new.json"

            params = {"limit": batch_size, "after": after, "raw_json": 1, "t": time_filter}

            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
                response.raise_for_status()
                logging.info("Subreddit/user posts request successful")
            except Exception as e:
                logging.info("Subreddit/user posts request unsuccessful: %s", e)
                print(f"Failed to fetch posts for subreddit/user {subreddit}: {e}")
                break

            data = response.json()
            posts = data["data"]["children"]
            if not posts:
                break

            for post in posts:
                post_data = post["data"]
                post_info = {
                    "title": post_data["title"],
                    "author": post_data["author"],
                    "permalink": post_data["permalink"],
                    "score": post_data["score"],
                    "num_comments": post_data["num_comments"],
                    "created_utc": post_data["created_utc"],
                }
                if post_data.get("post_hint") == "image" and "url" in post_data:
                    post_info["image_url"] = post_data["url"]
                elif "preview" in post_data and "images" in post_data["preview"]:
                    post_info["image_url"] = post_data["preview"]["images"][0]["source"]["url"]
                if "thumbnail" in post_data and post_data["thumbnail"] != "self":
                    post_info["thumbnail_url"] = post_data["thumbnail"]

                all_posts.append(post_info)
                total_fetched += 1
                if total_fetched >= limit:
                    break

            after = data["data"].get("after")
            if not after:
                break

            time.sleep(random.uniform(1, 2))
            logging.info("Sleeping for random time")

        logging.info("Successfully fetched subreddit posts for %s", subreddit)
        return all_posts

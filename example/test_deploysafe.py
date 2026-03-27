import os
import sys
import time

current_dir = os.path.dirname(os.path.abspath(__file__))
src_path = os.path.join(os.path.dirname(current_dir), "src")
sys.path.append(src_path)

from yars.yars import YARS
from yars.utils import export_to_csv, export_post_details_to_csv

scraper = YARS()

SUBREDDITS = ["netsec", "webdev", "devops", "cybersecurity", "AskNetsec"]
QUERY = "web app security vulnerability"
SINCE_UTC = int(time.time()) - (90 * 24 * 60 * 60)  # last 90 days

print("\n=== 1. Multi-subreddit post search ===")
posts = scraper.search_subreddits(
    subreddits=SUBREDDITS,
    query=QUERY,
    limit=10,
    time_filter="year",
    since_utc=SINCE_UTC,
    min_score=5,
    min_comments=2,
    pages=2,
    workers=5,
)
print(f"\nTotal posts found: {len(posts)}")
for p in posts[:3]:
    print(f"  [{p['score']}] {p['title'][:80]} (r/{p['subreddit']})")

export_to_csv(posts, "deploysafe_posts.csv")

print("\n=== 2. Multi-subreddit comment search ===")
comments = scraper.search_subreddits_comments(
    subreddits=SUBREDDITS,
    query=QUERY,
    limit=10,
    time_filter="year",
    since_utc=SINCE_UTC,
    min_score=3,
    pages=2,
    workers=5,
)
print(f"\nTotal comments found: {len(comments)}")
for c in comments[:3]:
    print(f"  [{c['score']}] {c['body'][:100]} (r/{c['subreddit']})")

export_to_csv(comments, "deploysafe_comments.csv")

print("\n=== 3. Scrape top post + export flat comment CSV ===")
if posts:
    top_post = sorted(posts, key=lambda p: p["score"], reverse=True)[0]
    print(f"Scraping: {top_post['title'][:80]}")
    permalink = top_post["link"].replace("https://www.reddit.com", "")
    details = scraper.scrape_post_details(permalink, max_comments=20, min_comment_score=2, max_depth=2)
    if details:
        export_post_details_to_csv(details, "deploysafe_thread.csv")
    else:
        print("Failed to scrape post details.")

print("\nDone. Check deploysafe_posts.csv, deploysafe_comments.csv, deploysafe_thread.csv")

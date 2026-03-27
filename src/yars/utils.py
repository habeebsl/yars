import os
import csv
import json
import logging
import requests
from urllib.parse import urlparse
from pygments import formatters, highlight, lexers

logging.basicConfig(
    level=logging.INFO, filename="YARS.log", format="%(asctime)s - %(message)s"
)


def display_results(results, title):

    try:
        print(f"\n{'-'*20} {title} {'-'*20}")

        if isinstance(results, list):
            for item in results:
                if isinstance(item, dict):
                    formatted_json = json.dumps(item, sort_keys=True, indent=4)
                    colorful_json = highlight(
                        formatted_json,
                        lexers.JsonLexer(),
                        formatters.TerminalFormatter(),
                    )
                    print(colorful_json)
                else:
                    print(item)
        elif isinstance(results, dict):
            formatted_json = json.dumps(results, sort_keys=True, indent=4)
            colorful_json = highlight(
                formatted_json, lexers.JsonLexer(), formatters.TerminalFormatter()
            )
            print(colorful_json)
        else:
            logging.warning(
                "No results to display: expected a list or dictionary, got %S",
                type(results),
            )
            print("No results to display.")

    except Exception as e:
        logging.error(f"Error displaying results: {e}")
        print("Error displaying results.")


def download_image(image_url, output_folder="images", session=None):

    os.makedirs(output_folder, exist_ok=True)

    filename = os.path.basename(urlparse(image_url).path)
    filepath = os.path.join(output_folder, filename)

    if session is None:
        session = requests.Session()

    try:
        response = session.get(image_url, stream=True)
        response.raise_for_status()
        with open(filepath, "wb") as f:
            for chunk in response.iter_content(8192):
                f.write(chunk)
        logging.info("Downloaded: %s", filepath)
        return filepath
    except requests.RequestException as e:
        logging.error("Failed to download %s: %s", image_url, e)
        return None
    except Exception as e:
        logging.error("An error occurred while saving the image: %s", e)
        return None


def export_to_json(data, filename="output.json"):
    try:
        with open(filename, "w", encoding="utf-8") as json_file:
            json.dump(data, json_file, indent=4)
        print(f"Data successfully exported to {filename}")
    except Exception as e:
        print(f"Error exporting to JSON: {e}")


def export_to_csv(data, filename="output.csv"):
    """Export a flat list of dicts to CSV. Works for post search and comment search results."""
    if not data:
        print("No data to export.")
        return
    try:
        keys = list(data[0].keys())
        with open(filename, "w", newline="", encoding="utf-8") as output_file:
            writer = csv.DictWriter(output_file, fieldnames=keys, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(data)
        print(f"Data successfully exported to {filename}")
    except Exception as e:
        print(f"Error exporting to CSV: {e}")


def _flatten_comments(comments, post_title, post_body, depth=0, parent_author=""):
    rows = []
    for comment in comments:
        rows.append(
            {
                "post_title": post_title,
                "post_body": post_body[:500],
                "depth": depth,
                "parent_author": parent_author,
                "author": comment.get("author", ""),
                "body": comment.get("body", ""),
                "score": comment.get("score", ""),
            }
        )
        replies = comment.get("replies", [])
        if replies:
            rows.extend(
                _flatten_comments(replies, post_title, post_body, depth + 1, comment.get("author", ""))
            )
    return rows


def export_post_details_to_csv(post_details, filename="comments.csv"):
    """Flatten a scrape_post_details result into one row per comment and export to CSV."""
    if not post_details:
        print("No data to export.")
        return
    try:
        rows = _flatten_comments(
            post_details.get("comments", []),
            post_details.get("title", ""),
            post_details.get("body", ""),
        )
        if not rows:
            print("No comments to export.")
            return
        keys = ["post_title", "post_body", "depth", "parent_author", "author", "body", "score"]
        with open(filename, "w", newline="", encoding="utf-8") as output_file:
            writer = csv.DictWriter(output_file, fieldnames=keys)
            writer.writeheader()
            writer.writerows(rows)
        print(f"{len(rows)} comments exported to {filename}")
    except Exception as e:
        print(f"Error exporting post details to CSV: {e}")
import argparse
import os
from datetime import datetime

from dotenv import load_dotenv

from ops_article import OperatorArticle
from ops_youtube import OperatorYoutube
from ops_rss import OperatorRSS


parser = argparse.ArgumentParser()
parser.add_argument("--prefix", help="runtime prefix path",
                    default="./run")
parser.add_argument("--start", help="start time",
                    default=datetime.now().isoformat())
parser.add_argument("--run-id", help="run-id",
                    default="")
parser.add_argument("--job-id", help="job-id",
                    default="")
parser.add_argument("--data-folder", help="data folder to save",
                    default="./data")
parser.add_argument("--sources", help="sources to pull, comma separated",
                    default=os.getenv("CONTENT_SOURCES", "Article,Youtube,RSS"))


def pull_article(args, op, source):
    """
    Pull from inbox - articles
    """
    print("######################################################")
    print("# Pull from Inbox - Articles")
    print("######################################################")

    data = op.sync(source)
    return data


def save_article(args, op, source, data):
    print("######################################################")
    print("# Save Articles to json file")
    print("######################################################")
    op.save2json(args.data_folder, args.run_id, "article.json", data)
    op.updateLastEditedTimeForData(data, source, "default")


def pull_youtube(args, op, source):
    """
    Pull from inbox - youtube
    """
    print("######################################################")
    print("# Pull from Inbox - Youtube")
    print("######################################################")

    data = op.sync(source)

    print(f"Pulled {len(data.keys())} youtube videos")
    return data


def save_youtube(args, op, source, data):
    print("######################################################")
    print("# Save Youtube transcripts to json file")
    print("######################################################")
    op.save2json(args.data_folder, args.run_id, "youtube.json", data)
    op.updateLastEditedTimeForData(data, source, "default")


def pull_rss(args, op, source):
    """
    Pull from rss
    """
    print("######################################################")
    print("# Pull from Inbox - RSS")
    print("######################################################")

    data = op.sync(source)

    print(f"Pulled {len(data.keys())} RSS articles")
    return data


def save_rss(args, op, source, data):
    print("######################################################")
    print("# Save RSS articles to json file")
    print("######################################################")
    op.save2json(args.data_folder, args.run_id, "rss.json", data)
    op.updateLastEditedTimeForData(data, source, "default")


def run(args):
    sources = args.sources.split(",")
    print(f"Sources: {sources}")

    for source in sources:
        print(f"Pulling from source: {source} ...")

        if source == "Article":
            op = OperatorArticle()
            data = pull_article(args, op, source)
            save_article(args, op, source, data)

        elif source == "Youtube":
            op = OperatorYoutube()
            data = pull_youtube(args, op, source)
            save_youtube(args, op, source, data)

        elif source == "RSS":
            op = OperatorRSS()
            data = pull_rss(args, op, source)
            save_rss(args, op, source, data)


if __name__ == "__main__":
    args = parser.parse_args()
    load_dotenv()

    run(args)

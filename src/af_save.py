import argparse
import os
from datetime import datetime

from dotenv import load_dotenv
import utils

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
parser.add_argument("--targets", help="targets to push, comma separated",
                    default="notion")
parser.add_argument("--topics-top-k", help="pick top-k topics to push",
                    default=3)
parser.add_argument("--categories-top-k", help="pick top-k categories to push",
                    default=3)
parser.add_argument("--dedup", help="whether dedup item",
                    default=True)
parser.add_argument("--min-score-to-rank",
                    help="The minimum relevant score to start ranking",
                    default=4)
parser.add_argument("--max-distance",
                    help="Max distance for similarity search, range [0.0, 1.0]",
                    default=0.5)


def process_article(args):
    print("#####################################################")
    print("# Process Article")
    print("#####################################################")
    op = OperatorArticle()

    data = op.readFromJson(args.data_folder, args.run_id, "article.json")
    data_deduped = op.dedup(data, target="toread")
    data_summarized = op.summarize(data_deduped)
    data_ranked = op.rank(data_summarized)

    targets = args.targets.split(",")
    pushed_stats = op.push(data_ranked, targets)

    return op.createStats(
        "Article",
        "",
        data,
        data_deduped=data_deduped,
        data_summarized=data_summarized,
        data_ranked=data_ranked,
        pushed_stats=pushed_stats)


def process_youtube(args):
    print("#####################################################")
    print(f"# Process Youtube, dedup: {args.dedup}")
    print("#####################################################")
    op = OperatorYoutube()

    data = op.readFromJson(args.data_folder, args.run_id, "youtube.json")
    data_deduped = data
    need_dedup = utils.str2bool(args.dedup)
    if need_dedup:
        data_deduped = op.dedup(data, target="toread")
    else:
        data_deduped = [x for x in data.values()]

    data_summarized = op.summarize(data_deduped)
    data_ranked = op.rank(data_summarized)

    targets = args.targets.split(",")
    pushed_stats = op.push(data_ranked, targets)

    return op.createStats(
        "YouTube",
        "",
        data,
        data_deduped=data_deduped,
        data_summarized=data_summarized,
        data_ranked=data_ranked,
        pushed_stats=pushed_stats)


def process_rss(args):
    print("#####################################################")
    print(f"# Process RSS, dedup: {args.dedup}")
    print("#####################################################")
    op = OperatorRSS()

    data = op.readFromJson(args.data_folder, args.run_id, "rss.json")
    data_deduped = data

    need_dedup = utils.str2bool(args.dedup)
    if need_dedup:
        data_deduped = op.dedup(data, target="toread")
    else:
        data_deduped = [x for x in data.values()]

    data_scored = op.score(
        data_deduped,
        start_date=args.start,
        max_distance=args.max_distance)

    # Only pick top 1 to reduce the overflow
    data_filtered = op.filter(data_scored, k=1, min_score=4)
    data_summarized = op.summarize(data_filtered)

    targets = args.targets.split(",")
    pushed_stats = op.push(data_summarized, targets)

    return op.createStats(
        "RSS",
        "",
        data,
        data_deduped=data_deduped,
        data_scored=data_scored,
        data_filtered=data_filtered,
        data_summarized=data_summarized,
        pushed_stats=pushed_stats)


def run(args):
    sources = args.sources.split(",")
    stats = []

    for source in sources:
        print(f"Pushing data for source: {source} ...")

        if source == "Article":
            stat = process_article(args)

        elif source == "Youtube":
            stat = process_youtube(args)

        elif source == "RSS":
            stat = process_rss(args)

        stats.extend(stat)

    # Print stats
    print("#####################################################")
    print("# Stats")
    print("#####################################################")
    for stat in stats:
        stat.print()


if __name__ == "__main__":
    args = parser.parse_args()
    load_dotenv()

    run(args)

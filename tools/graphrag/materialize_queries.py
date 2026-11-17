"""Materialize canonical query JSONL records from a raw dataset file.

The benchmark and embed_queries.py both consume this JSONL (one record per query with a
stable id, question and answer). It is a required intermediate artifact of the from-scratch
workflow: raw datasets are not passed to the benchmark directly.
"""
import argparse

from tools.graphrag.eval.retrieval import materialize_queries


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', required=True,
                        help='Raw dataset file (zip, json, jsonl or parquet)')
    parser.add_argument('--output', required=True, help='Destination query JSONL path')
    args = parser.parse_args()
    count = materialize_queries(args.source, args.output)
    print('wrote %d queries to %s' % (count, args.output))


if __name__ == '__main__':
    main()

#!/usr/local/bin/python

from elasticsearch import Elasticsearch, helpers, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import argparse
import sys
reload(sys)
sys.setdefaultencoding("utf-8")


def parse_args():
    parser = argparse.ArgumentParser(description='Elasticsearch management')
    parser.add_argument('-r', '--reindex', action='store', help='Reindex all documents in specified index and append with "-reindex", if --new_index_name options has not been specified')
    parser.add_argument('-n', '--new_index_name', action='store', help='Name for new index')
    parser.add_argument('-d', '--delete_index', action='store', help='Specify which index to delete')
    parser.add_argument('-e', '--endpoint', action='store', help='Specify Elasticsearch host', required=True)
    parser.add_argument('-a', '--access_key', action='store', help='AWS Access Key')
    parser.add_argument('-s', '--secret_key', action='store', help='AWS Secret Key')
    args = parser.parse_args()
    return args


def es():
    # Creates the connection with Elasticsearch
    args = parse_args()

    host = args.endpoint
    access_key = args.access_key
    secret_key = args.secret_key
    conn = Elasticsearch(
        hosts=[{'host': host,
                'port': 443}],
        http_auth=AWS4Auth(access_key, secret_key, "us-east-1", 'es'),
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )

    return conn


def delete():
    # To delete an index specified
    args = parse_args()

    conn = es()
    index_to_remove = args.delete_index

    conn.indices.delete(index=index_to_remove)
    print(index_to_remove + "has been removed")


def reindex():
    # To reindex a specified index and appends the new index with "-reindex" if the --new_index_name options has not been specified
    args = parse_args()

    src_index_name = args.reindex

    if args.new_index_name is not None:
        des_index_name = args.new_index_name
    else:
        des_index_name = src_index_name + "-reindex"

    helpers.reindex(es(), src_index_name, des_index_name)
    print(src_index_name + " has been reindexed to " + des_index_name)


def main():
    args = parse_args()

    if args.delete_index:
        delete()
    elif args.reindex:
        reindex()
    else:
        sys.exit()
    pass

if __name__ == '__main__':
    main()

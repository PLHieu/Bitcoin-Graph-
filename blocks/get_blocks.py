import requests
import json

sys.path.append(os.path.abspath(os.path.abspath(os.path.dirname("__file__"))))
from utils.db import db


def get_block(s, e):
    for i in range(s, e+1):
        response = requests.get(f"https://blockchain.info/rawblock/{i}")
        result =  json.loads(response.text)
        res = db["raw_block"].replace_one({'block_index': result.get("block_index")}, result, upsert=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--start', help="start block")
    parser.add_argument('-e', '--end', help="end block")
    args = parser.parse_args()


    if args.start and args.end:
        get_block(args.start, args.end)
    else:
        raise Exception("Arguments is invalid")



from update import nytimes
from update import atlantic

import argparse

def update_all(env='local'):
    nytimes.run_update(env)
    atlantic.run_update(env)

if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument('-e', '--env', default='local', choices=['local', 'prod'], help='environment this command is running in; local or prod')
    argz = args.parse_args()

    update_all(env=argz.env)

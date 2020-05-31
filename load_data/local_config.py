## Local path to NYTimes data repositor. '~'s are acceptable.
nytimes_github_path = '~/Documents/github/nytimes-covid-data'

## path to DATA repo -- this is used to fuel Observable Maps
data_repo_path = '~/Documents/github/kaogilvie.github.io'

## In order to import the covid_utils libraries correctly, you must
## add the covid repo to your PYTHONPATH. (You can also do this in your
## .bashrc, .bash_profile, or .profile if you'd like). This block will
## run upon import into any scripts.
path_to_this_repo = '~/Documents/github/local-covid-data'
import sys
import os
sys.path.insert(0, os.path.expanduser(path_to_this_repo))

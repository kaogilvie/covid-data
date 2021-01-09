## Local path to data repositories. '~'s are acceptable.

github_info = {
    'nytimes': {
        'git_file_path': '~/nytimes-covid-data',
        'git_url' : 'git@github.com:nytimes/covid-19-data.git'
    },
    'atlantic': {
        'git_file_path': '~/atlantic-covid-data',
        'git_url' : None
    },
}

## path to DATA repo -- this is used to fuel Observable Maps
data_repo_path = '~/kaogilvie.github.io'

## In order to import the covid_utils libraries correctly, you must
## add the covid repo to your PYTHONPATH. (You can also do this in your
## .bashrc, .bash_profile, or .profile if you'd like). This block will
## run upon import into any scripts.
path_to_this_repo = '~/local-covid-data'
import sys
import os
sys.path.insert(0, os.path.expanduser(path_to_this_repo))

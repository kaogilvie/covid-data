import os
import warnings
import logging.config

def get_path_to_proj_root(proj_name):
    """
    Assumes the top level directory is named "proj_name".
    """
    path_to_utils = os.path.realpath(__file__)
    list_of_path = path_to_utils.split('/')
    if list_of_path[0] == '':
        list_of_path = list_of_path[1:]
    true_path = ''
    for part in list_of_path:
        if part == proj_name:
            break
        else:
            true_path = true_path+'/'+part
    true_path = true_path+'/'+proj_name
    return true_path


def configure_logging(proj_name=False, dev=False, log_path=False):
    """
    Sets up standard "useful-for-development" root logger.
    Proj_name is the name of the folder the project resides in
    (and oline will expand the absolute path for it), or is an
    absolute filepath itself.
    Call "getLogger" after this configuration has been set
    up in order to get an instance of this logger.
    """
    if proj_name is not False:
        path = get_path_to_proj_root(proj_name)
        log_path = path+'/'+proj_name+'/logs/dev.log'
        if log_path is not False:
            warnings.warn("Log_path will not be used if proj_name is populated.", SyntaxWarning)
    else:
        if log_path is False:
            raise Exception("Must provide either proj_name or log_path!!")

    if dev is True:
        logging_config = {
                    'formatters': {
                        'standard': {
                            'format': '%(asctime)s: %(levelname)s: %(message)s'
                        },
                    },
                    'handlers': {
                        'fileHandler': {
                            'level': 'DEBUG',
                            'class': 'logging.FileHandler',
                            'filename': log_path,
                            'formatter': 'standard'
                        },
                    },
                    'loggers': {
                        '': {
                            'handlers': ['fileHandler'],
                            'level': 'INFO',
                            'propagate': True,
                        },
                    },
                    'version': 1,
                }
    else:
        logging_config = {
                    'formatters': {
                        'standard': {
                            'format': '%(asctime)s: %(levelname)s: %(message)s'
                        },
                    },
                    'handlers': {
                        'console': {
                            'level': 'DEBUG',
                            'class': 'logging.StreamHandler',
                            'formatter': 'standard'
                        },
                    },
                    'loggers': {
                        '': {
                            'handlers': ['console'],
                            'level': 'INFO',
                            'propagate': True,
                        },
                    },
                    'version': 1,
                }
    logging.config.dictConfig(logging_config)

import re
import time
from urllib.parse import urlparse
from pathlib import Path
import os
import pandas as pd

from loguru import logger


class Container:
    def __init__(self):
        pass

    def __str__(self):
        return ''.join(['{}: {}\n'.format(k, v) for k, v in self.__dict__.items()])


def url_to_domain(url):
    # a function to turn a string url into the bare domain string
    # take url, splib by dot, and take second rightmost constituent
    domain = urlparse(url).netloc.split('.')[-2]
    return domain


def html_clean_1(s):
    # a specific cleaner for (ronorp) html strings
    if isinstance(s, str):  # don't try to apply regex to timestamps....
        s = re.sub(r'\n', '', s)  # remove newlines
        s = re.sub(' +', ' ', s)  # rmove excessive space
        s = s.strip()
    return s


def time_difference(start, end):
    diff = (end - start)
    return diff.seconds


def wait_minimum(f=0, last=None, ):
    # just like time.sleep(f), but shortens the sleeptime in case other processes
    # took their time since last invocation
    if last:
        now = time.time()
        sleeptime = max(f - (now - last), 0)
        time.sleep(sleeptime)
    return time.time()


def loop_logger(i, n, message):
    if (i % n == 0) and (i != 0):
        pass


class Looplogger:

    def __init__(self, n, message):
        self.n = n
        self.counter = 0
        self.state = 10
        logger.info(message)

    def log(self, i):
        #  print(i,self.counter,self.state,self.counter/self.n < self.state/100,(self.counter+1)/self.n >= self.state/100)
        if (self.counter / self.n < self.state / 100) and ((self.counter + 1) / self.n >= self.state / 100):
            print(f'{self.state}% ', end='')
            self.state += 10
        self.counter += 1


def safe_drop(df, dropcandidates):
    # safely drop columns from dfs
    actualcols = df.columns
    dropcols = [col for col in dropcandidates if col in actualcols]

    return df.drop(columns=dropcols)


def lib_path_maker(folder=Path(''),name='',dom=''):
    path = folder / ('_'.join([name,dom]) + '.csv')
    return path


def load_df_safely(path):
    if os.path.isfile(path):
        lib = pd.read_csv(path)
    else:
        lib = pd.DataFrame()
    return lib

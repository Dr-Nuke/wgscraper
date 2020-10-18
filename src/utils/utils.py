import re
from urllib.parse import urlparse


class Container:
    def __init__(self):
        pass

    def __str__(self):
        return ''.join(['{}: {}\n'.format(k, v) for k, v in self.__dict__.items()])


def url_to_domain(url):
    # a function to turn a string url into the bare domain string
    domain = urlparse(url).netloc  # results in www.abc.xy
    domain = domain.replace('www.', '')
    pos = domain.rfind('.')  # find rightmost dot
    if pos >= 0:  # found one
        domain = domain[:pos]
    return domain


def html_clean_1(s):
    # a specific cleaner for (ronorp) html strings
    if isinstance(s, str):  # don't try to apply regex to timestamps....
        s = re.sub(r'\n', '', s)  # remove newlines
        s = re.sub(' +', ' ', s)  # rmove excessive space
        s = s.strip()
    return s

def time_difference(start,end):
    diff = (end - start)
    return diff.seconds

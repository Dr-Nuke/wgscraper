from urllib.parse import urlparse

class container:
    def __init__(self):
        pass

    def __str__(self):
        return ''.join(['{}: {}\n'.format(k,v) for k,v in self.__dict__.items()])

def url_to_domain(url):
    # a function to turn a string url into the bare domain string
    domain = urlparse(url).netloc  # results in www.abc.xy
    domain = domain.replace('www.', '')
    pos = domain.rfind('.')  # find rightmost dot
    if pos >= 0:  # found one
        domain = domain[:pos]
    return domain
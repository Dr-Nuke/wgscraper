import datetime
import os
from pathlib import Path
import sys

from loguru import logger
import configparser

configpath = Path(r'C:\coding\wgscraper\configs\config.py').parent
sys.path.append(str(configpath))
from config import config

import src.wgscraper.data.wrapper as W


if __name__ == '__main__':
    lofgilepath = Path(r'C:\coding\wgscraper\logs')
    logger.add(lofgilepath / "logfile{time}.log")
    logger.info('starting main scraping')

    s = W.Scrape(config)
    s.scrape()
    s.process()
    s.unify()
    s.analyze()


print('end')

# todo sort out encoding. should always be utf-8
# todo: clean duplicated results from scraping (incl. previous entries) , 'last'
# todo: make an archive to dump old entries of any table
# todo: reduce ronorp scraping junk appearing in the df
# todo: add configurable websites i.i. which one to scrape
# todo: get proper status bar for longerprocessings
# todo: make headerless
# todo: make config an argument, as well as the pipeline
# todo: search the new entries for matches & do email automation
# todo: fix unnamed:0 from save/load csv
# todo: add owner to database (processing)
# todo: learn squaremeters
# todo: investigate, how updates of an ad affect it
# todo: fix timestamp :seems to confuse monthe & days
# todo: fix
# todo: add login to fetch contact details
# toto: add owner (ronorp, wgzimmer)
# toto: fix driver stop vs driver quit


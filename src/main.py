import datetime
import json
import os
from pathlib import Path

import data.process as process
import data.scrape as scrape
from loguru import logger

from configs.config import base_urls, driverpath

if __name__ == '__main__':
    # config:
    logger.add("logfile{time}.log")
    logger.info('starting main scraping')
    # make sure base urls have "https://"
    configfile = Path(os.getcwd()) / 'configs' / 'config.json'
    configs_from_disk = json.load(configfile)

    config = {'urls': base_urls,  # ["https://www.ronorp.net/zuerich/immobilien/wg-zuerich.1220?s=1"]
              'root': Path(os.getcwd()),
              'driverpath': driverpath,  # [/path/to/geckodriver-v0.27.0-win64/geckodriver.exe]
              'vault': 'room_database.csv',
              'processed': 'processed_data.csv',
              'force_reprocessing': False,
              'forced_cutoff': datetime.datetime.now() - datetime.timedelta(days=14),
              }

    s = scrape.main(config)
    logger.info('scraping completed')
    t = process.main(config)
    logger.info('processing completed')

# todo: learn squaremeters
# todo: make database
# todo: get timestamp
# todo: remove miliseconds from scrape_ts
# todo: convert numerics (room, rent,...) to numbers
# todo: investigate, how updates of an ad affect it



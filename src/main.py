import datetime
import os
from pathlib import Path


import data.process as process
import data.scrape as scrape
import data.postprocess as postp
from loguru import logger

from configs.config import base_urls, driverpath

if __name__ == '__main__':

    logger.add("logfile{time}.log")
    logger.info('starting main scraping')

    # make sure base urls contain "https://"
    config = {'urls': base_urls,  # ["https://www.ronorp.net/zuerich/immobilien/wg-zuerich.1220?s=1"]
              'root': Path(os.getcwd()),
              'driverpath': driverpath,  # [/path/to/geckodriver-v0.27.0-win64/geckodriver.exe]
              'vault': 'room_database.csv',
              'processed': 'processed_data.csv',
              'post_processed': 'processed_data.csv',
              'force_reprocessing': False,
              'forced_cutoff': datetime.datetime.now() - datetime.timedelta(days=14),
              }

    # s = scrape.main(config)
    logger.info('scraping completed')
    t = process.main(config)
    logger.info('processing completed')
    p = postp.main(config)

# todo: learn squaremeters

# todo: convert numerics (room, rent,...) to numbers
# todo: investigate, how updates of an ad affect it

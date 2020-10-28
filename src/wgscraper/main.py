import datetime
import os
from pathlib import Path


from loguru import logger

import src.wgscraper.data.analysis as analysis
import src.wgscraper.data.postprocess as postp
import src.wgscraper.data.process as process
import src.wgscraper.data.scrape as scrape
# from configs.config import base_urls, driverpath, searchdict


if __name__ == '__main__':




    logger.add("logfile{time}.log")
    logger.info('starting main scraping')

    # make sure base urls contain "https://"
    config = {'urls': base_urls,  # ["https://www.ronorp.net/zuerich/immobilien/wg-zuerich.1220?s=1"]
              'root': Path(os.getcwd()).parent.parent,
              'driverpath': driverpath,  # [/path/to/geckodriver-v0.27.0-win64/geckodriver.exe]
              'vault': 'room_database.csv',
              'processed': 'processed_data.csv',
              'post_processed': 'post_processed_data.csv',
              'results_archive': 'results_archive.csv',
              'force_reprocessing': False,
              'forced_cutoff': datetime.datetime.now() - datetime.timedelta(days=14),
              'searchdict': searchdict,  # [{'column': 'text', 'pattern': 'spacious', 'flag': 'IGNORECASE'}]
              'now': datetime.datetime.now().replace(microsecond=0),
              }

    s = scrape.main(config)
    t = process.main(config)
    p = postp.main(config)
    a = analysis.main(config)

print('end')
# todo: make config an argument, as well as the pipeline
# todo: search the new entries for matches & do email automation
# todo: fix unnamed:0 from save/load csv
# todo: add owner to database (processing)
# todo: learn squaremeters
# todo: investigate, how updates of an ad affect it
# todo: fix timestamp :seems to confuse monthe & days
# todo: fix
# todo: add login to fetch contact details

import os
from pathlib import Path

import data.scrape as scrape
from loguru import logger

if __name__ == '__main__':
    # config:
    logger.info('starting main scraping')
    base_urls = ["https://www.ronorp.net/zuerich/immobilien/wg-zuerich.1220?s=1",
                 ]
    driverpath = Path(r'C:\Users\Bo-user\Downloads\geckodriver-v0.27.0-win64') / 'geckodriver.exe'

    config = {'urls': base_urls,
              'root': Path(os.getcwd()),
              'driverpath': driverpath,
              }
    s = scrape.Scraper(config)
    s.scrape()
    logger.info('scraping completed')

    a=1

import datetime
import os
from pathlib import Path

import data.process as process
import data.scrape as scrape
from loguru import logger

if __name__ == '__main__':
    # config:
    logger.add("logfile{time}.log")
    logger.info('starting main scraping')
    # make sure base urls have "https://"
    base_urls = ["https://www.ronorp.net/zuerich/immobilien/wg-zuerich.1220?s=1",
                 "https://www.wgzimmer.ch/wgzimmer/search/mate.html",
                 "https://flatfox.ch/en/search/?east=8.593843904245384&north=47.43131788418717&south=47.33779350769794&west=8.468599767811336"
                 "https://www.urbanhome.ch/suchen/mieten/wohnen/wg/zh"
                 "https://en.comparis.ch/immobilien/marktplatz/zuerich/wg-zimmer/mieten",
                 "https://www.students.ch/wohnen/list",
                 "https://www.immoscout24.ch/en",
                 "https://housing.justlanded.com/en/Switzerland_Zurich/Flatshare",
                 "https://www.nestpick.com/search?city=zurich&display=grid&order=relevance&page=1&map=3&currency=CHF&location=85682309",
                 "https://www.homegate.ch/rent/apartment/city-zurich/matching-list",
                 ]
    driverpath = Path(r'C:\Users\Bo-user\Downloads\geckodriver-v0.27.0-win64') / 'geckodriver.exe'

    config = {'urls': base_urls,
              'root': Path(os.getcwd()),
              'driverpath': driverpath,
              'vault': 'room_database.csv',
              'processed': 'processed_data.csv',
              'force_reprocessing': False,
              'forced_cutoff': datetime.datetime.now() - datetime.timedelta(days=14),
              }

    # s = scrape.main(config)
    logger.info('scraping completed')
    t = process.main(config)
    logger.info('processing completed')



# todo: learn squaremeters
# todo: make database

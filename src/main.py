import os
from pathlib import Path

import data.scrape as scrape
from loguru import logger

if __name__ == '__main__':
    # config:
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
              'vault_2': 'room_database_2.csv'
              }

    s = scrape.main(config)
    # s = scrape.Scraper(config)
    # s.scrape()
    logger.info('scraping completed')

# todo: implement unique id of ads. title is insufficient. 1st try: url
# todo: download only those ads that are not yet in the database

# todo: go to second page, then nth page

# todo: learn squaremeters
# todo: make database
# todo: make one scraper object per website

# save pages as individual files, along with a database entry. only extract id (original url),
# timestamp

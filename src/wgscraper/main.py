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
                 "https://www.kraftwerk1.ch/freie-objekte-neu/freieobjekte.html",
                 ]

    driverpath = Path(r'C:\Users\Bo-user\Downloads\geckodriver-v0.27.0-win64') / 'geckodriver.exe'

    textpatterns = [
        r'mehralswohnen',
        r'dialogweg',
        r'kraftwerk',
        r'hochfoif',
        r'mehr als 11',
        r'mehrals11',
        r'd6s4-SUED',
        r'Hunzkomune',
        r'Bunter Haufen',
        r'BunterHaufen',
        r'BuntGemischt',
        r'Bunt-Gemischt',
        r'Brückenwohnen',
        #     r'MIR',
        r'HUNZREAL',
        r'kalkbreite',
        r'hunziker',
        r'Mehrgeneration',
        r'kraftwerk1',
        r'suite55',
        r'suite37',
        r'gross-wg',

    ]

    addresspatterns = [
        r'dialogweg',
        r'hunziker'
    ]

    titlepatterns = textpatterns

    searchdict = [
        *[{'column': 'text', 'pattern': pat, 'flag': 'IGNORECASE'} for pat in textpatterns],
        *[{'column': 'address', 'pattern': pat, 'flag': 'IGNORECASE'} for pat in addresspatterns],
        *[{'column': 'title', 'pattern': pat, 'flag': 'IGNORECASE'} for pat in titlepatterns]
        #     {'column':'address', 'pattern': r'dialogweg','flag':'IGNORECASE'},
        #     {'column':'address', 'pattern': r'hunziker','flag':'IGNORECASE'},

    ]
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

    # s = scrape.main(config)
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

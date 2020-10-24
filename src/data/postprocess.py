import os

import pandas as pd
from loguru import logger
import src.utils.utils as u


class Postprocessor:
    # a simple class to host the postprocessing
    def __init__(self, config):
        self.root = config['root']
        self.datapath = self.root / 'data'
        self.input = self.datapath / config['processed']
        self.output = self.datapath / config['post_processed']

        if os.path.isfile(self.input):
            self.df_in = pd.read_csv(self.input)
        else:
            logger.info(f'no input data on {self.input}')
            return None

        if os.path.isfile(self.output):
            self.df_out = pd.read_csv(self.output)
        else:
            self.df_out = pd.DataFrame()
        # self.force_reprocessing = config['force_reprocessing']
        # self.forced_cutoff = config['forced_cutoff']

    def postprocess(self):
        logger.info(f'starting post-processing {self.input}')
        # first, let's get all the new stuff
        # if not self.force_reprocessing:
        # Either Identify what is new in the input
        if 'title' in self.df_out.columns:
            key_diff = set(self.df_in.alt).difference(self.df_out.title)
            df_to_do = self.df_in[self.df_in['alt'].isin(key_diff)]
            n=len(self.df_in)
            n_attempt=len(key_diff)
            ignored = len(self.df_in) - n_attempt
            logger.info(f'attempting to post-process {n_attempt} of {n} entries, ignoring {ignored} existing entries')
        else:
            df_to_do = self.df_in
            logger.info(f'{self.output} not found. post processing all entries')
        # else:
        #     # or take what came after the cutoff
        #     df_to_do = self.df_in[self.df_in['scrape_ts'] > self.forced_cutoff]

        if len(df_to_do) == 0:
            logger.info(f'no new entries for post processig in {self.input}')
            return
        post_processors = [self.postprocess_ronorp,
                           ]
        fails = []
        processed = []
        for func in post_processors:
            df_processed, df_to_do, df_fail = func(df_to_do)
            fails.append(df_fail)
            processed.append(df_processed)

        df_failed = pd.concat(fails)
        logger.info(f'failed to post-process {len(df_failed)} entries')
        logger.info(f'for {len(df_to_do)} entries there is no post-prcoessor')

        self.df_out = pd.concat([self.df_out, df_processed])
        ordercols=['timestamp', 'scrape_ts', 'domain', 'rooms', 'cost', 'address', 'duration',
                   'bid_ask', 'href', 'title', 'details', 'text', 'category1' ]
        self.df_out = self.df_out[ordercols]
        self.df_out.to_csv(self.output, index=False)
        logger.info(f'added {len(df_processed)} entries to {self.output}')
        logger.info('postprocessing completed')

    def postprocess_ronorp(self, df):
        if len(df) == 0:
            logger.info('no entries for ronorp to process')
            return df, df, df
        ind_ronorp = df['domain'] == 'ronorp'
        df_processed = df[ind_ronorp]
        df_to_do = df[~ind_ronorp]


        # renaming
        renames = {'alt': 'title',
                   }
        df_processed.rename(columns=renames, inplace=True)

        # make numericals
        df_processed['cost'] = pd.to_numeric(df_processed['cost'].str.replace("'", ''), errors='coerce')

        return df_processed, df_to_do, pd.DataFrame()


def main(config):
    logger.info('starting post processing')
    p = Postprocessor(config)
    p.postprocess()

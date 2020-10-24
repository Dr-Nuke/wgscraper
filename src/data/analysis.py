import os

import pandas as pd
from loguru import logger
import src.utils.utils as u


class Analysis:
    # a simple class to host the analysis & search
    def __init__(self, config):
        self.root = config['root']
        self.datapath = self.root / 'data'
        self.input = self.datapath / config['post_processed']
        self.outputpath = self.datapath / 'searchresults'
        if not os.path.isdir(self.outputpath):
            os.makedirs(self.outputpath)

        if os.path.isfile(self.input):
            self.df_in = pd.read_csv(self.input)
        else:
            logger.info('no input data')
            return None

    def analyse(self):
        logger.info(f'starting analysing {self.input}')
        # first, let's get all the new stuff
        # if not self.force_reprocessing:
        # Either Identify what is new in the input
        if 'title' in self.df_out.columns:
            key_diff = set(self.df_in.alt).difference(self.df_out.title)
            df_to_do = self.df_in[self.df_in.index.isin(key_diff)]
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
        self.df_out.to_csv(self.output, index=False)
        logger.info(f'added {len(df_processed)} entries to {self.output}')
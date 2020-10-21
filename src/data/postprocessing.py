import pandas as pd


class Postprocessor:
    # a simple class to host the postprocessing
    def __init__(self, config):
        self.root = config['root']
        self.datapath = self.root / 'data'
        self.input = self.datapath / config['processed']
        self.output = self.datapath / config['processed']

        if os.path.isfile(self.input):
            self.df_in = pd.read_csv(self.input)
        else:
            logger.info('no input data')
            return None

        if os.path.isfile(self.output):
            self.df_out = pd.read_csv(self.output)
        else:
            self.df_out = pd.DataFrame()
        self.force_reprocessing = config['force_reprocessing']
        self.forced_cutoff = config['forced_cutoff']

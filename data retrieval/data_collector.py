import json
import requests
import time
import pandas as pd
from json_parser import *

class idescat_API:
    def __init__(self, batch_size=100):
        self.create_meta_municipalities()
        self.not_yet_collected = self.meta
        self.batch_size = batch_size

    def pop_not_yet_collected(self, mun_ids):
        for mun in mun_ids:
            self.not_yet_collected.pop(mun)

    def collect_batch(self):
        mun_ids = list(self.not_yet_collected.keys())
        if len(mun_ids) > self.batch_size:
            mun_ids = mun_ids[:self.batch_size]
        mun_dict = {ids: self.not_yet_collected[ids] for ids in mun_ids}
        self.pop_not_yet_collected(mun_ids)
        return self.collect_n_muns(mun_dict)

    def request_mun(self, mun_id):
        mun_res = requests.get("https://api.idescat.cat/emex/v1/dades.json?tipus=mun?id={}&lang=en".format(mun_id))
        return mun_res.json()

    def create_meta_municipalities(self):
        """
        Retrieves dictionairy containing all municipalities from the idescat. Structure of the dict:
            key: id of the mun
            value: name of the mun
        """
        self.meta = {}
        mun_res = requests.get("https://api.idescat.cat/emex/v1/dades.json?lang=en")
        mun_meta = mun_res.json()
        muns = mun_meta['fitxes']['cols']['col']
        for mun in muns:
            self.meta[mun['id']] = mun['content']
        return self.meta

    def collect_n_muns(self, meta):
        """
        Collects all the data of each municipality, making a request every n seconds
        """
        start_time = time.time()
        last_time = time.time()
        municipality_dict = {}
        total = len(meta.keys())
        estimated_time = total * 0.23
        failed = {}
        print(f'The estimated time to download the amount of data is {estimated_time/60} minutes')
        for i, identifier in enumerate(meta.keys()):
            try:
                municipality_dict[identifier] = self.request_mun(identifier)
            except:
                print(f'Failed to obtain data on {identifier}, trying in the last batch again...')
                failed[identifier] = meta[identifier]
            if i == 0:
                last_time = time.time()
                print(f'({i}/{total}). Data from {identifier} was downloaded in {last_time - start_time} seconds')
            else:
                now = time.time()
                print(f'({i}/{total}). Data from {identifier} was downloaded in {now - last_time} seconds')
                last_time = now
        print("Retrieved data of {} municipalities in {} seconds".format(i ,int(time.time()-start_time)))

        if len(failed) > 0:
            self.not_yet_collected = {**self.not_yet_collected, **failed}

        return municipality_dict

    def collect_and_parse_data(self):
        batch = 1
        while len(self.not_yet_collected) > 0:
            json_data = self.collect_batch()
            json_parser = JSONParser(json_value='v', json_column_name=('c', 'calt'))
            json_parser.add_value_map(value_splitter)
            json_parser.add_key_map(homogenize_key)
            add_muns_to_parser(json_parser, json_data)
            json_parser.parse_data()
            pd_dict = change_dicts_to_panda_df(json_parser.containers())
            df = concatenate_dict_of_dfs(pd_dict)
            df.to_csv(f'../data/data_{batch}.csv')
            batch = batch + 1
            time.sleep(5)


def add_muns_to_parser(parser, muns):
    for name, mun in muns.items():
        parser.add_json(d=mun, name=name)


def change_dicts_to_panda_df(dicts):
    pd_dict = {}
    for name, dct in dicts.items():
        df = pd.DataFrame(dct, index=[name])
        pd_dict[name] = df
    return pd_dict


def concatenate_dict_of_dfs(dfs):
    for i, (name, d) in enumerate(dfs.items()):
        if i == 0:
            df = d
        else:
            df = pd.concat([df, d])
    return df

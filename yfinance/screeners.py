#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas
import requests


def _get_screener(url):

    def cast_types(val):

        if isinstance(val, str):
            val = val.replace(',', '')
            
        if type(val) in [float, int]:
            return float(val)
        elif 'M' in val:
            return float(val.strip('M')) * 1e6
        elif 'B' in val:
            return float(val.strip('B')) * 1e9
        elif 'T' in val:
            return float(val.strip('T')) * 1e12
        elif '%' in val:
            return float(val.strip('%'))
        else:
            return float(val)

    def wrapper():

        output, pull, rows, i = [], True, 200, 0
        while pull:
            params = dict(offset=i*rows, count=rows)
            table = pandas.read_html(requests.get(url, params=params)\
                                     .content)[0]
            output.append(table)
            i += 1
            if table.size < rows:
                pull = False

        output = pandas.concat(output)
        output.drop(columns='52 Week Range', inplace=True)

        for column in output:
            if column in ['Symbol', 'Name']:
                continue
            output[column] = output[column].map(cast_types)

        return output

    return wrapper

def _screeners():

    BASE_URL = 'https://ca.finance.yahoo.com/screener/'
    SCREENERS = BASE_URL + 'predefined/'

    table = pandas.read_html(requests.get(BASE_URL).content)[0]
    names = []
    for name in table[0]:
        name = name.replace('-', '')
        names.append('_'.join(name.split()).lower())

    functions = {name.upper(): _get_screener(SCREENERS + name)
                 for name in names}
    globals().update(functions)


_screeners()

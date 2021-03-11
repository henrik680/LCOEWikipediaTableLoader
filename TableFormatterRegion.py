import logging

logging.getLogger().setLevel(logging.INFO)

def reformat_EIA(df):
    col_names = []
    for c in df.columns:
        name: str = ''
        if type(c) == type(()):
            for x in c: name += ' ' + x.strip()
        col_names.append(name.strip())
    df.columns = col_names
    df.drop(df.tail(2).index, inplace=True)
    logging.debug('reformat_EIA: df={}'.format(df))
    return df

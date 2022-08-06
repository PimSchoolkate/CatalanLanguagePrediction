def merge_csvs(path, name):
    import os
    import pandas as pd
    
    df = pd.DataFrame()
    for file in os.listdir(path):
        if 'data_' not in file:
            continue
        filepath = os.path.join(path, file)
        subdf = pd.read_csv(filepath)
        df = pd.concat([df, subdf], axis=0)

    df.to_csv(os.path.join(path, name), index=False)
    return

#####

def get_info_from_column(col):
    import re
    
    col = col.replace('Cata', 'cata')\
        .replace('Spa', 'spa')\
        .replace('Rate', 'rate')\
        .replace('Parl', 'parl')\
        .replace('Cong', 'cong')\
        .replace('Europ', 'europ')\
        .replace('Depu', 'depu')\
        .replace('Social', 'social')\
        .replace('Security', 'security')\

    fields = re.split(r'(?=_[A-Z][^A-Z])', col)
    return fields

def format_name(string):
    import re
    
    return '_'.join(re.findall(r'[A-Za-z0-9]+', string)).lower()

def get_relevant_info_by_group(col):

    fields = get_info_from_column(col)
    l = len(fields)
    main = fields[0].lower()
    
    if 'culture' in main: sub = fields[2]; ind = fields[-1]
    elif 'economic' in main: sub = fields[1]; ind = fields[-1]
    elif 'education' in main: sub = fields[1]; ind = fields[-1]
    elif 'election' in main: sub = fields[1]; ind = fields[-1]
    elif 'environment' in main: sub = fields[1]; ind = fields[-1]
    elif 'labour' in main:
        sub = fields[1]
        if fields[1] != fields[4]:
            sub = fields[1] + fields[-4]
        ind = fields[-1]  
    elif 'main' in main: sub = fields[2]; ind = fields[-1]
    elif 'population' in main:
        sub = fields[1]
        if len(fields) >= 4:
            sub = '_'.join([fields[2], fields[3]])
        ind = fields[-1]     
    elif 'quality' in main: sub = fields[1]; ind = fields[-1]
    elif 'territory': sub = ''; ind = fields[-1]
    else: sub = ''; ind = ''
        
    return format_name(main), format_name(sub), format_name(ind)
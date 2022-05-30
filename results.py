from operator import index
import os
import pandas as pd

path = os.getcwd()+"/results/"
keys = ['BAYER', 'HOECHST ', 'Samsung ', 'LG ', 'Lucent ', 'United  ']

for i in range(len(keys)):
    cit = pd.read_csv('all_citations_with_dates.txt', sep='|')
    cit = cit.loc[cit['key'] == keys[i]]
    cit['citation_date'] = cit['citation_date'].apply(lambda x: pd.to_datetime(str(x), format='%Y-%m-%d'))
    count = cit.value_counts(subset=['citation_date', 'app_name','app_nbr'])
    count = count.to_frame()
    count.reset_index(inplace=True)
    count.rename(columns={0:'counts'}, inplace=True)
    count.to_csv(path+str(keys[i])+'.txt', index=False, sep='|')
    count.sort_values(by='citation_date', inplace=True)
    count.to_csv(path+str(keys[i])+'_sorted_by_time.txt', index=False, sep='|')

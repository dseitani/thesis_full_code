import os
import pandas as pd
import time
from fuzzywuzzy import fuzz

path1 = os.getcwd()+"/data/Total_citations_per_company_with_nbr_of_patents.txt"
path2 = os.getcwd()+"/data/Cited_app_nbr_freq_count_with_names.txt"
path3 = os.getcwd()+"/fuzzy/"
start = time.time()

#remove companies with few citations (~<20)
cit = pd.read_csv(path1, sep='|')
cit.drop(cit.index[10000:], inplace=True)
cit.to_csv("Total_citations_per_company_with_nbr_of_patents_rows_10000.txt", index=False, sep='|')

#keep only company names
comp = cit[['app_name']]
comp.sort_values(by='app_name', inplace=True)
comp.to_csv('company_names_all.txt', index=False, header=False)

#open file
cit = pd.read_csv("Total_citations_per_company_with_nbr_of_patents_rows_10000.txt", sep='|')

# remove strings such as "Corporation" with make the fuzzy search difficult
with open('useless_strings.txt', 'r') as us:
    us_str = us.read().splitlines()

cit.replace(to_replace= us_str, value="", inplace=True, regex=True)
cit.to_csv("company_names_total_clean.txt", index=False, sep='|')

#begin fuzzy search
comp = cit['app_name'].to_list()
new = pd.DataFrame(columns=['app_name', 'matches', 'indexes'])

match_names = []
indexes = []

for i in range(len(comp)):
    indexes.append(i+2)
    for j in range(len(comp)-1-i):
        match = fuzz.token_set_ratio(comp[i], comp[j+i+1])
        if match >= 85:
            match_names.append(comp[j+i+1])
            indexes.append(j+i+1+2)
    new = new.append({'app_name': comp[i], 'matches': match_names, 'indexes':indexes}, ignore_index=True)
    match_names = []
    indexes = []

new.to_csv('company_names_clean_matching.txt', index=False, sep='|')

print("time = %s" % (time.time() - start))

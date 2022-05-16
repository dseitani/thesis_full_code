import os
import pandas as pd
import time
from fuzzywuzzy import fuzz

path1 = "Total_citations_per_company_with_nbr_of_patents_rows_10000.txt"
path2 = 'company_names_clean_matching.txt'

def remove(company,n):
    names = sums['matched_names'][n-2]
    cit = 0
    pat = 0
    if type(company) is list:
        for i in company:
            x = tot[tot['app_name'] == i].index.values[0]
            cit += tot['citations'][x]
            pat += tot['patents'][x]
            if i in names:
                names.remove(i)
    else:
        x = tot[tot['app_name'] == company].index.values[0]
        cit += tot['citations'][x]
        pat += tot['patents'][x]
        if company in names:
            names.remove(company)
    sums.at[n-2, 'total_citations'] = sums.at[n-2, 'total_citations'] - cit
    sums.at[n-2, 'total_patents'] = sums.at[n-2, 'total_patents'] - pat

def keep(company,n):
    names = sums['matched_names'][n-2]
    cit = 0
    pat = 0
    for i in company:
        x = tot[tot['app_name'] == i].index.values[0]
        cit += tot['citations'][x]
        pat += tot['patents'][x]
    sums['matched_names'][n-2] = company
    sums.at[n-2, 'total_citations'] = cit
    sums.at[n-2, 'total_patents'] = pat

def add(n1,n2):
    sums['matched_names'][n1-2] = sums['matched_names'][n1-2] + sums['matched_names'][n2-2]
    cits = sums['total_citations'][n1-2] + sums['total_citations'][n2-2]
    pats = sums['total_patents'][n1-2] + sums['total_patents'][n2-2]
    sums['total_citations'][n1-2] = cits
    sums['total_citations'][n2-2] = 0
    sums['total_patents'][n1-2] = pats
    sums['total_patents'][n2-2] = 0

start = time.time()

tot = pd.read_csv(path1, sep='|')
comp = pd.read_csv(path2, sep='|')

#turn indexes of matches to ints from strings
comp['indx'] = ''

for i in comp.index:
    if comp['indexes'][i] != '[]':
        indxs = list(map(int, comp['indexes'][i].strip('[]').split(',')))
        comp['indx'][i] = indxs
    else:
        comp['indx'][i] = []

comp.drop('indexes', inplace=True, axis=1)

#group indexes that belong to same company and omit rows that have been matched
comp['all_indx'] = ''
comp['marked'] = ''
indxes = comp['indx'].to_list()

for i in range(len(indxes)):
    if len(indxes[i]) != 1:
        nextin = []
        for j in indxes[i][1:]:
            nextin = nextin + indxes[j-2]
            comp['marked'][j-2] = 1
        l = list(set(nextin + indxes[i]))
        l.sort()
        comp['all_indx'][i] = l
    else:
        comp['all_indx'][i] = indxes[i]

comp.drop(comp[comp['marked'] == 1].index, inplace = True)  
comp.drop(columns=['matches','indx','marked'], inplace = True)

#also get the names of all the matches
f = pd.read_csv(path2, sep='|')
comp['matched'] = ''
for i in comp.index:
    matches = []
    indxs = comp['all_indx'][i]
    if len(indxs) != 1:
        for j in indxs:
            matches.append(f.loc[j-2, 'app_name'])  
    else:
        matches.append(comp['app_name'][i])
    comp['matched'][i] = matches   

#do second fuzzy to find inconsistencies
for i in comp.index:
    name = comp['app_name'][i]
    matches = comp['matched'][i]
    ind = comp['all_indx'][i]
    if len(matches) != 1:
        for j in reversed(range(len(matches))):
            m = fuzz.token_set_ratio(name, matches[j])
            if m <= 85:
                del ind[j]
                del matches[j]

comp.to_csv('indexes.txt', index=False, sep='|')

#citations and patent sums
sums = pd.DataFrame(columns=['app_name', 'total_citations', 'total_patents', 'matched_names','key'])

for i in comp.index:
    cits = 0
    pats = 0
    matches = []
    indxs = comp['all_indx'][i]
    if len(indxs) != 1:
        for j in indxs:
            cits += tot.loc[j-2,'citations']
            pats += tot.loc[j-2, 'patents']
            matches.append(tot.loc[j-2, 'app_name'])
        sums = sums.append({'app_name': tot.loc[i, 'app_name'], 'matched_names': matches,
                            'total_citations': cits, 'total_patents': pats, 'key': comp['app_name'][i]}, ignore_index=True)
    else:
        sums = sums.append({'app_name': tot.loc[i, 'app_name'], 'matched_names': tot.loc[i, 'app_name'],
                            'total_citations': tot.loc[i, 'citations'], 'total_patents': tot.loc[i, 'patents'], 'key': comp['app_name'][i]}, ignore_index=True)

sums.sort_values(by='total_citations', ascending=False, inplace=True)
sums.reset_index(inplace=True, drop=True)
sums.to_csv('total_citations_and_patents.txt', columns=['app_name', 'total_citations', 'total_patents'], index=False, sep='|')
sums.to_csv('company_names_with_matches.txt', columns=['app_name', 'matched_names','key'], index=False, sep='|')

#remove noted errors

#row 2, remove
siemens = ['Nokia Siemens Networks Oy','Nokia Siemens Networks GmbH & Co. KG', \
    'Nokia Siemens Networks S.p.A.', 'BSH Bosch und Siemens Hausgeräte GmbH', \
        'Bosch-Siemens Hausgeräte GmbH']
remove(siemens,2)

#row 3, remove
canon = 'CANYON CORPORATION'
remove(canon, 3)

#row 42, remove
texas = ['THE TEXAS A&M UNIVERSITY SYSTEM', 'BOARD OF REGENTS THE UNIVERSITY OF TEXAS SYSTEM', \
    'EXPANDABLE GRAFTS PARTNERSHIP a Texas General Partnership', 'BOARD OF REGENTS, THE UNIVERSITY OF TEXAS SYSTEM', \
        'Board of Regents, The University of Texas System', 'TEXAS UNITED CHEMICAL COMPANY, LLC.', 'Texas Industries Inc.']
remove(texas, 42)

#row 30, remove
sharp = 'Merck Sharp & Dohme Corp.'
remove(sharp, 30)

#row 74, keep
denso = ['DENSO CORPORATION', 'Denso Corporation', 'Denso Wave Incorporated']
keep(denso, 74)

#row 24, keep
united = ['United Technologies Corporation', 'UNITED TECHNOLOGIES CORPORATION']
keep(united, 24)

#row 6, remove
matsu = 'NITTO ELECTRIC INDUSTRIAL CO., LTD.'
remove(matsu, 6)

#row 60, remove
dow = ['Dow Corning Toray Silicone Company, Limited', 'Dow Corning Toray Silicone Co., Ltd.', \
       'Dow Corning Toray Silicone Company, Ltd.', 'Dow Corning Toray Silicone Company Ltd.', \
       'Dow Corning Toray Co., Ltd.', 'Dow Corning Toray Silicone Company Limited']
remove(dow, 60)

# add row 6 (Matsushita) to row 36 (Panasonic)
add(6, 36)

# add row 27 (FUJI PHOTO FILM) to row 52 (FUJIFILM)
add(27, 52)

# add row 46 (Honda giken) to row 86 (Hondas Motors)
add(46, 86)

# add BSH
bsh = ['BSH Bosch und Siemens Hausgeräte GmbH','Bosch-Siemens Hausgeräte GmbH']
cit = 0
pat = 0
for i in bsh:
    x = tot[tot['app_name'] == i].index.values[0]
    cit += tot['citations'][x]
    pat += tot['patents'][x]

sums = sums.append({'app_name': 'BSH Bosch und Siemens Hausgeräte GmbH', 'matched_names': bsh,
                    'total_citations': cit, 'total_patents': pat, 'key': 'BSH'}, ignore_index=True)

# add Dow Toray
cit = 0
pat = 0
for i in dow:
    x = tot[tot['app_name'] == i].index.values[0]
    cit += tot['citations'][x]
    pat += tot['patents'][x]

sums = sums.append({'app_name': 'Dow Corning Toray Silicone Company, Limited', 'matched_names': dow,
             'total_citations': cit, 'total_patents': pat, 'key': 'DOW TORAY'}, ignore_index=True)

#to get the correct order after the above modifications
sums.sort_values(by='total_citations', ascending=False, inplace=True)

#keep only first 100 rows
sums.drop(sums.index[100:], inplace=True)
sums.to_csv('total_citations_and_patents_first_100.txt', columns=[
            'app_name', 'total_citations', 'total_patents'], index=False, sep='|')
sums.to_csv('company_names_with_matches_first_100.txt', columns=[
            'app_name', 'matched_names','key'], index=False, sep='|')

print("time = %s" % (time.time() - start))

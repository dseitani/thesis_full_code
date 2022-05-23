import os
import pandas as pd
import time
import matplotlib.pyplot as plt

path1 = os.getcwd()+"/data/EPO_App_reg.txt"
path2 = os.getcwd()+"/data/EPO_CITATIONS.txt"
path3 = 'company_names_with_matches_first_100.txt'
moving = os.getcwd()+"/plots/moving_average/"
total = os.getcwd()+"/plots/total/"

start = time.time()

reg = pd.read_csv(path1, sep='|', usecols=['app_nbr','app_name'])
cit = pd.read_csv(path2, sep='|', usecols=['Citing_pub_date', 'Cited_App_nbr'])
pat = pd.read_csv(path2, sep='|', usecols=['Citing_pub_date', 'Citing_app_nbr'])
comp = pd.read_csv(path3, sep='|')

#comp['key'].to_csv('name_keys.txt', sep='|', index=False)
keys = comp['key'].to_list()

#turn matches column from str to list
comp['matches'] = ''

for i in comp.index:
    if len(comp['matched_names'][i]) != 1:
        if i == 37:     # L'Oreal needs special treatment thanks to the apostrophe in the name
            match = list(map(str, comp['matched_names'][i].strip('[]').split('",')))
            mmatch = [j.replace('"', '') for j in match]
        else:
            match = list(map(str, comp['matched_names'][i].strip('[]').split("',")))
            mmatch = [j.replace("'", "") for j in match]
        mmmatch = [j.replace(" ", "",1) for j in mmatch[1:]]
        mmmatch.insert(0,mmatch[0])
        comp['matches'][i] = mmmatch
    else:
        comp['matches'][i] = comp['matched_names'][i]

comp.drop('matched_names', inplace=True, axis=1)

#create basic times series 
app = pd.DataFrame(columns=['app_nbr', 'app_name','key'])
fi = 0
for i in comp.index:
    matches = comp['matches'][i]
    key = comp['key'][i]
    for j in matches:
        rows = reg.loc[reg['app_name'] == j]
        app = pd.concat([app, rows], ignore_index=True)
    li = app.index[-1]+1
    app['key'].iloc[fi:li] = key
    fi = li

#time series for citations
timesercit = app.merge(cit, left_on='app_nbr', right_on='Cited_App_nbr', how='inner')
timesercit['citation_date'] = timesercit['Citing_pub_date'].apply(
    lambda x: pd.to_datetime(str(x), format='%Y%m%d'))
timesercit.drop(columns=['Cited_App_nbr','Citing_pub_date'], axis=1, inplace=True)
timesercit.sort_values(by=['key', 'citation_date'],inplace=True, ignore_index=True)
timesercit.to_csv('all_citations_with_dates.txt', index=False, sep='|')

#time series for patents
timeserapp = app.merge(pat, left_on='app_nbr',right_on='Citing_app_nbr', how='inner')
timeserapp['publication_date'] = timeserapp['Citing_pub_date'].apply(
    lambda x: pd.to_datetime(str(x), format='%Y%m%d'))
timeserapp.drop(columns=['Citing_app_nbr', 'Citing_pub_date'], axis=1, inplace=True)
timeserapp.sort_values(by=['key', 'publication_date'],inplace=True, ignore_index=True)
timeserapp.drop_duplicates(subset='app_nbr',inplace=True, ignore_index=True)
timeserapp.to_csv('all_patents_with_dates.txt', index=False, sep='|')

#create trimester intevals 
timesercit = pd.read_csv('all_citations_with_dates.txt',sep='|', usecols=['key', 'citation_date'])
timesercit['citation_date'] = timesercit['citation_date'].apply(
    lambda x: pd.to_datetime(str(x), format='%Y-%m-%d'))
timesercit['citations'] = 1
timesercit.set_index('citation_date', inplace=True)

timeserapp = pd.read_csv('all_patents_with_dates.txt',sep='|', usecols=['key', 'publication_date'])
timeserapp['publication_date'] = timeserapp['publication_date'].apply(
    lambda x: pd.to_datetime(str(x), format='%Y-%m-%d'))
timeserapp['patents'] = 1
timeserapp.set_index('publication_date', inplace=True)

time_cit = pd.DataFrame(columns=['company_name', 'citation_date', 'citations'])
time_pat = pd.DataFrame(columns=['company_name', 'publication_date', 'patents'])

for i in keys:
    dfc = timesercit.copy()
    dfp = timeserapp.copy()
    dfc = dfc.loc[dfc['key']==i, 'citations']
    dfp = dfp.loc[dfp['key'] == i, 'patents']
    dfc = dfc.resample('Q').sum()
    dfp = dfp.resample('Q').sum()
    dfc = dfc.to_frame().reset_index()
    dfp = dfp.to_frame().reset_index()
    dfc['company_name'] = i
    dfp['company_name'] = i
    time_cit = time_cit.append(dfc)
    time_pat = time_pat.append(dfp)

time_cit.to_csv('timeseries_quarters.txt', sep='|', index=False)
time_pat.to_csv('timeseries_quarters_pats.txt', sep='|', index=False)

# number of citations for each patent with applicatin number
cit_freq = pd.read_csv('all_citations_with_dates.txt', sep='|', usecols=['app_nbr','key','app_name'])
patents = cit_freq['app_nbr'].value_counts()
patents.rename('citations',inplace=True)
patents.index.name = 'app_nbr'
cits = cit_freq.merge(patents, on='app_nbr', how='inner')
cits.drop_duplicates(keep='first', inplace=True)
cits.sort_values(by=['key','citations'], ascending=False, inplace=True)
cits.to_csv('citations_of_every_patent.txt', index=False, sep='|')

#find mean for each quarter 
by_quarters = pd.read_csv('timeseries_quarters.txt', sep='|')
by_quarters['citation_date'] = by_quarters['citation_date'].apply(lambda x: pd.to_datetime(str(x), format='%Y-%m-%d'))
by_quarters.set_index('citation_date', inplace=True)
quarter_mean = by_quarters.resample('Q').mean()
quarter_mean.rename(columns={'citations': 'mean'}, inplace=True)

pat_quarters = pd.read_csv('timeseries_quarters_pats.txt', sep='|')
pat_quarters['publication_date'] = pat_quarters['publication_date'].apply(lambda x: pd.to_datetime(str(x), format='%Y-%m-%d'))
pat_quarters.set_index('publication_date', inplace=True)
pat_mean = pat_quarters.resample('Q').mean()
pat_mean.rename(columns={'patents': 'mean'}, inplace=True)

#find moving average and variance 
# moving average (SMA) for citations
sma = by_quarters.copy()
sma = sma.resample('Q').mean()
sma = sma.rolling(window=4).mean()
sma.rename(columns={'citations':'mean'}, inplace=True)
sma.fillna(0, inplace=True)
sma.to_csv('moving_average.txt', sep='|')

# plot of SMA and mean
plt.clf()
ax = sma.plot(label='moving average')
quarter_mean['mean'].plot(ax=ax, label='mean')
plt.title('Simple Moving Average of Citations Mean')
plt.xlabel('Citation date')
plt.ylabel('Value')
plt.legend()
plt.savefig("sma_cit.png", dpi=300)
plt.close()

# moving average and variance for each company
comp_ma = pd.DataFrame(columns=['company_name','citation_date','citations'])
pat_ma = pd.DataFrame(columns=['company_name','publication_date','patents'])
comp_va = pd.DataFrame(columns=['company_name', 'variance'])
for i in range(len(keys)):
    df_cit = by_quarters.copy()
    df_pat = pat_quarters.copy()
    df_cit = df_cit.loc[df_cit['company_name'] == keys[i], 'citations']
    df_pat = df_pat.loc[df_pat['company_name'] == keys[i], 'patents']
    ma_cit = df_cit.rolling(window=4).mean()
    ma_pat = df_pat.rolling(window=4).mean()
    va_cit = ma_cit.var(ddof=0)
    ma_cit.fillna(0, inplace=True)
    ma_pat.fillna(0, inplace=True)
    ma_cit = ma_cit.to_frame()
    ma_pat = ma_pat.to_frame()
    ma_cit.reset_index(inplace=True)
    ma_pat.reset_index(inplace=True)
    comp_ma = pd.concat([comp_ma, ma_cit], ignore_index=True)
    pat_ma = pd.concat([pat_ma, ma_pat], ignore_index=True)
    comp_ma.fillna(keys[i], inplace=True)
    pat_ma.fillna(keys[i], inplace=True)
    comp_va = comp_va.append({'company_name': keys[i], 'variance':va_cit}, ignore_index=True)
comp_va.sort_values(by='variance', ascending=False, inplace=True)
comp_va.to_csv('variance_companies.txt',sep='|',index=False)

# deviation from moving arerage mean
ma_dev = comp_ma.merge(sma, on='citation_date', how='inner')
pat_cits = ma_dev.merge(pat_ma, left_on=['company_name', 'citation_date'], \
    right_on=['company_name', 'publication_date'], how='left')
ma_dev.sort_values(by=['company_name', 'citation_date'], inplace=True)
ma_dev['deviation'] = ma_dev['citations'] - ma_dev['mean']
ma_dev.set_index('citation_date', inplace=True)
ma_dev.to_csv('moving_average_companies.txt', sep='|')

# plots for each company
pat_cits.sort_values(by=['company_name', 'citation_date'], inplace=True)
pat_cits.set_index('citation_date', inplace=True)
pat_cits.drop('publication_date', axis=1, inplace=True)
pat_cits.fillna(0, inplace=True)
pat_cits.to_csv('citations_and_patents.txt',sep='|')

names = pd.read_csv('name_keys.txt', sep='|')
for i in names.index:
    plt.clf()
    df = pat_cits.copy()
    df = df.loc[df['company_name'] == names['key'][i]]
    df.plot()
    plt.title(str(names['official_name'][i]))
    plt.xlabel('Date')
    plt.ylabel('Value')
    plt.legend()
    plt.savefig(total+str(names['key'][i])+".png", dpi=300)
    plt.close('all') 
 
print("time = %s" % (time.time() - start))

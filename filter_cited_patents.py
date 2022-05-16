import os
import pandas as pd

path1 = os.getcwd()+"/data/202007_EPO_CITATIONS.txt"
path2 = os.getcwd()+"/data/EPO_CITATIONS.txt"
path3 = os.getcwd()+"/data/EPO_CITATIONS_sorted.txt"
path4 = os.getcwd()+"/data/Cited_app_nbr_freq_count_sorted_by_app.txt"
path5 = os.getcwd()+"/data/Cited_app_nbr_freq_count.txt"
path6 = os.getcwd()+"/data/EPO_App_reg.txt"
path7 = os.getcwd()+"/data/Cited_app_nbr_freq_count_with_names.txt"
path9 = os.getcwd()+"/data/Number_of_cited_patents_per_company.txt"
path10 = os.getcwd()+"/data/Total_citations_per_company.txt"
path11 = os.getcwd()+"/data/Total_citations_per_company_with_nbr_of_patents.txt"

# keep only EPO cited patents 
with open(path1) as cit, open(path2, "w") as ncit:
    for line in cit:
        words = line.split('|')
        if (words[6] == 'EP'):
            ncit.write(line)

df = pd.read_csv(path2, sep='|')

#frequency count of 'Cited_App_nbr' (just change 'app' to 'pub' for 'cited_pub_nbr')
fq = df['Cited_App_nbr'].value_counts()
fq.to_csv(path5, header=False, sep='|')

#sort by app_nbr
app = pd.read_csv(path5, sep='|',names=['Cited_app_nbr','citations'])
app.sort_values(by='Cited_app_nbr', inplace=True)
app.to_csv(path4, sep='|', index=False)

#read regpat for company names and reg_codes
reg = pd.read_csv(path6, sep='|', names=['app_nbr','app_name','reg_code','cntry_code'])
nam = app.merge(reg, left_on='Cited_app_nbr', right_on='app_nbr', how='inner')
nam.drop(columns='app_nbr', axis=1, inplace=True)
nam.to_csv(path7, sep='|', index=False)
cod = nam[['app_name', 'reg_code']]

#get total citations for each unique company name
grou = nam.groupby(['app_name']).sum()
grou.sort_values(by='citations', ascending=False, inplace=True)
grou.to_csv(path10,sep='|')

#for each company how many of its patents have been cited
coun = nam['app_name'].value_counts() 
coun = coun.to_frame().reset_index()
coun.rename(columns={'index':'app_name','app_name':'patents'}, inplace=True)
coun.to_csv(path9, index=False, sep='|')

#the above two together
tot = grou.merge(coun, on='app_name', how='inner')
tot.to_csv(path11, index=False, sep='|')

#get company names with their reg_codes
with_cod = grou.merge(cod, on='app_name', how='inner')
with_cod.drop_duplicates(keep='first', inplace=True)
with_cod.drop(columns='citations', axis=1, inplace=True)
with_cod.sort_values(by='app_name', inplace=True)
with_cod.to_csv('company_names_all_with_codes.txt', index=False,sep='|')

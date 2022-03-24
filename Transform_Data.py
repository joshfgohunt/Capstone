import pandas as pd


def group_house_dfs(df):
    df = df.groupby(['State','Year','size'])['value'].agg(['count', 'mean', 'median', 'std', 'min', 'max'])
    return df

def return_merged_count(df_arr):
    merged_count_df = pd.concat(df_arr, join='outer', axis=1).fillna(0)
    merged_count_df.columns = ['min','max', 'mean', 'std', 'median']
    return merged_count_df
######High_End###########
def return_nlargest_state_count(df,n, col):
    ndf = df.nlargest(n,col).dropna()
    ndf = ndf.groupby(level=[0]).size()
    return ndf


def return_high_end(df):
     return df[df['State'].isin(['CA', 'MA', 'FL','NY', 'HI'])]
    
    
    
######Low_End###########
def return_nsmallest_state_count(df,n, col):
    ndf = df.nsmallest(n,col).dropna()
    ndf = ndf.groupby(level=[0]).size()
    return ndf


def return_cheap_end(df):
     return df[df['State'].isin(['MO', 'MT', 'AR','KY', 'OK'])]
    
    

######Mid-Tier###########


def return_mid_tier(df):
     return df[df['State'].isin(['ID', 'IL', 'IN','TX', 'TN', 'GA', 'AZ','MI'])]
    
    

######Population###########
def return_city_pop_grouped(df):
    df = df.groupby(['State'])['Mean_Pop'].agg(['mean', 'count', 'median', 'std', 'min', 'max'])
    return df

def get_top_10(df):
    df10 = df.groupby('State').apply(lambda x: x.sort_values(["Mean_Pop"], ascending = False)).reset_index(drop=True).groupby('State').head(10)
    return df10

def get_mean_top_10(df):
    df = df.groupby(['State'])['Mean_Pop'].mean()
    return df



######Degree_Categorical###########
def get_degree_sex(df):
    df = df.groupby(['State','Sex'])['Bachelor\'s Degree Holders'].sum()
    df = df.unstack()
    df['Ratio'] = df['Female']/ df['Male']
    return df

def get_degree_age(df):
    df = df.groupby(['State','Age Group'])['Bachelor\'s Degree Holders'].sum()
    df = df.unstack()
    df['young_workers'] = df['25 to 39']/ df['25 and older']
    df['old_workers'] = df['65 and older']/ df['25 and older']
    age_stacked = df.sort_values(by='young_workers',ascending=False)
    return age_stacked

def get_degree_group(df):
    df = df.groupby( ['State']).sum()
    df['sci_eng'] = df['Science and Engineering'] / df['Bachelor\'s Degree Holders']
    df['sci_eng_rel'] = df['Science and Engineering Related Fields'] / df['Bachelor\'s Degree Holders']
    df['business'] = df['Business'] / df['Bachelor\'s Degree Holders']
    df['education'] = df['Education'] / df['Bachelor\'s Degree Holders']
    df['art_hum_oth'] = df['Arts, Humanities and Others'] / df['Bachelor\'s Degree Holders']
    df = df.sort_values(by='sci_eng',ascending=False)
    return df
######### Degree Counts
def create_jobs(df, val):
    title = 'job_title_' + str(val)
    count = 'job_title_'+str(val)+'_count'
    job = df[['major',title, count]]
    job.columns = ['major', 'title', 'count']
    return job
def get_degree_count(merge):
    totals = merge[['major','count']]
    totals['title'] = 'total' 
    job1 = create_jobs(merge, 1)
    job2 = create_jobs(merge, 2)
    job3 = create_jobs(merge, 3)
    job4 = create_jobs(merge, 4)
    job5 = create_jobs(merge, 5)
    job6 = create_jobs(merge, 6)
    job7 = create_jobs(merge, 7)
    job8 = create_jobs(merge, 8)
    job9 = create_jobs(merge, 9)
    job10 = create_jobs(merge, 10)
    job_list = totals.append(job1)
    job_list = job_list.append(job2)
    job_list = job_list.append(job3)
    job_list = job_list.append(job4)
    job_list = job_list.append(job5)
    job_list = job_list.append(job6)
    job_list = job_list.append(job7)
    job_list = job_list.append(job8)
    job_list = job_list.append(job9)
    job_list = job_list.append(job10)
    job_list_grp =job_list.groupby(['major', 'title']).apply(lambda x: x.sort_values(["count"], ascending = False)).reset_index(drop=True)
    job_list_grp = job_list_grp[['major', 'title', 'count']]
    return job_list_grp


###########################
def add_state_abbrev(df, left):
    us_state_abbrev = {
'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR', 'California': 'CA', 'Colorado': 'CO',
'Connecticut': 'CT', 'Delaware': 'DE', 'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI', 'Idaho': 'ID',
'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS', 'Kentucky': 'KY', 'Louisiana': 'LA',
'Maine': 'ME', 'Maryland': 'MD', 'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS',
'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV', 'New Hampshire': 'NH', 'New Jersey': 'NJ',
'New Mexico': 'NM', 'New York': 'NY', 'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH', 'Oklahoma': 'OK',
'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC', 'South Dakota': 'SD',
'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT', 'Vermont': 'VT', 'Virginia': 'VA', 'Washington': 'WA',
'West Virginia': 'WV', 'Wisconsin': 'WI', 'Wyoming': 'WY'}
    
    us_abbr = pd.DataFrame.from_dict(us_state_abbrev, orient='index')
    us_abbr = us_abbr.reset_index()
    us_abbr.columns = ['State', 'Abbr'] 
    right = 'Abbr'
    
    df = df.merge(us_abbr, how='inner', left_on=left, right_on=right)
    df = df[['City', 'StateAbbreviation', 'County', 'State']]
    return df

def add_state_region(df, left):
    state_region = {'Alabama': 'Southern', 'Alaska': 'Western', 'Arizona': 'Western', 'Arkansas': 'Southern', 'California': 'California', 'Colorado': 'Western',
'Connecticut': 'Northeastern', 'Delaware': 'Southern', 'Florida': 'Southern', 'Georgia': 'Southern', 'Hawaii': 'Western', 'Idaho': 'Western',
'Illinois': 'Midwestern', 'Indiana': 'Midwestern', 'Iowa': 'Midwestern', 'Kansas': 'Midwestern', 'Kentucky': 'Southern', 'Louisiana': 'Southern',
'Maine': 'Northeastern', 'Maryland': 'Southern', 'Massachusetts': 'Northeastern', 'Michigan': 'Midwestern', 'Minnesota': 'Midwestern', 'Mississippi': 'Southern',
'Missouri': 'Midwestern', 'Montana': 'Western', 'Nebraska': 'Midwestern', 'Nevada': 'Western', 'New Hampshire': 'Northeastern', 'New Jersey': 'Northeastern',
'New Mexico': 'Western', 'New York': 'Northeastern', 'North Carolina': 'Southern', 'North Dakota': 'Midwestern', 'Ohio': 'Midwestern', 'Oklahoma': 'Southern',
'Oregon': 'Western', 'Pennsylvania': 'Northeastern', 'Rhode Island': 'Northeastern', 'South Carolina': 'Southern', 'South Dakota': 'Midwestern',
'Tennessee': 'Southern', 'Texas': 'Southern', 'Utah': 'Western', 'Vermont': 'Northeastern', 'Virginia': 'Southern', 'Washington': 'Western',
'West Virginia': 'Southern', 'Wisconsin': 'Midwestern', 'Wyoming': 'Western'}
    state_region = pd.DataFrame.from_dict(state_region, orient='index')
    state_region = state_region.reset_index()
    state_region.columns = ['State', 'Region'] 
    right = 'State'
    print(df.head(5))
    df = df.merge(state_region, how='outer', left_on=left, right_on=right)
    return df
import pandas as pd
# set desired cols

# get subset of data with cols

# convert column names


def get_subset(fpath, column_subset):
    pd_data = pd.read_csv(fpath, usecols=column_subset)
    return pd_data
########## Location
def get_rent_df():
    cols = ["RegionName", 
                     "2017-02", "2019-12"]
    df1 = get_subset('Datasets/House Price/State_MedianRentalPrice_1Bedroom.csv', cols);
    df2 = get_subset('Datasets/House Price/State_MedianRentalPrice_2Bedroom.csv', cols);
    df3 = get_subset('Datasets/House Price/State_MedianRentalPrice_3Bedroom.csv', cols);
    df4 = get_subset('Datasets/House Price/State_MedianRentalPrice_4Bedroom.csv', cols);
    df1['size'] = '1br'
    df2['size'] = '2br'
    df3['size'] = '3br'
    df4['size'] = '4br'
    
    merged_df = df1.append(df2, ignore_index = True)
    merged_df = merged_df.append(df3, ignore_index = True)
    merged_df = merged_df.append(df4, ignore_index = True)
    
    changecols = {"2017-02": "2017",  "2019-12": "2020", "RegionName" :"State"}
    
    merged_df = merged_df.rename(columns = changecols)

    idvars = ['State', 'size']
    merged_rent_df = merged_df.melt(id_vars=idvars, var_name='Year')

    merged_rent_df = merged_rent_df.dropna(subset=['value'])
    merged_rent_df = merged_rent_df[merged_rent_df['State'] != 'Puerto Rico']
    merged_rent_df.to_csv('Final_Data/ETL/zillow_rental_prices.csv', index = False)
    return merged_rent_df

def get_zhvi_array():
    cols = ["RegionName", "2017-01-31", "2020-01-31"]
    df1 = get_subset('Datasets/House Price/State_Zhvi_1Bedroom.csv', cols);
    df2 = get_subset('Datasets/House Price/State_Zhvi_2Bedroom.csv', cols);
    df3 = get_subset('Datasets/House Price/State_Zhvi_3Bedroom.csv', cols);
    df4 = get_subset('Datasets/House Price/State_Zhvi_4Bedroom.csv', cols);
    df1['size'] = '1br'
    df2['size'] = '2br'
    df3['size'] = '3br'
    df4['size'] = '4br'

    merged_df = df1.append(df2, ignore_index = True)
    merged_df = merged_df.append(df3, ignore_index = True)
    merged_df = merged_df.append(df4, ignore_index = True)

    changecols = {"2017-01-31": "2017",  "2020-01-31": "2020", "RegionName" :"State"}
    merged_df = merged_df.rename(columns = changecols)

    idvars = ["State", 'size']
    merged_zhvi_df = merged_df.melt(id_vars=idvars, var_name='Year')
    merged_zhvi_df = merged_zhvi_df.dropna(subset=['value'])
    merged_zhvi_df = merged_zhvi_df[merged_zhvi_df['State'] != 'Puerto Rico']
    merged_zhvi_df.to_csv('Final_Data/ETL/zillow_house_prices.csv', index = False)
    return merged_zhvi_df

def get_state_days():
    state_days = pd.read_csv('Datasets/House Price/DaysOnZillow_State.csv')
    state_days = state_days[['RegionName',
       '2017-01', '2020-01']]
    changecols = {'RegionName': 'State', 
         '2017-01': '2017', '2020-01': '2020', }
    state_days = state_days.rename(columns = changecols)
    idvars = ['State']
    state_days = state_days.melt(id_vars=idvars, var_name='Year')
    state_days = state_days.dropna(subset = ['value'])
    state_days = state_days[state_days['State'] != 'Puerto Rico']
    state_days.to_csv('Final_Data/ETL/zillow_days_to_sell.csv', index = False)
    return state_days

def get_population_data():
    cols = ['STNAME','POPESTIMATE2017','POPESTIMATE2020']
    df = get_subset('Datasets/Population/2020_Us_Population_By_City.csv', cols);
    changecols_2 = { 'STNAME': 'State', 'POPESTIMATE2017': '2017',
    'POPESTIMATE2020': '2020'}
    df = df.rename(columns = changecols_2)
    
    idvars = ["State"]
    df = df.melt(id_vars=idvars, var_name='Year')
    df = df.groupby(['State', 'Year']).sum().reset_index()
    df.to_csv('Final_Data/ETL/state_population_counts.csv', index = False)
    return df

def state_agegroup_degree_majors():
    cols = [ 'Sex', 'Age Group','State', 
       'Science and Engineering', 'Science and Engineering Related Fields',
       'Business', 'Education', 'Arts, Humanities and Others']
    df = get_subset('Datasets/Regional Education/Bachelor_Degree_Majors.csv', cols)
    
    changecols = {"Science and Engineering Related Fields" :"Sci_Eng_Related"}
    df = df.rename(columns = changecols)
    df = df[df['Age Group'] != '25 and older']
    fields = ['Science and Engineering','Sci_Eng_Related','Business','Education','Arts, Humanities and Others']
    
    df[fields] = df[fields].apply(pd.to_numeric)
    df = df[df['Sex'] !='Total']
    idvars = ['Sex', 'Age Group','State']
    df = df.melt(id_vars=idvars, var_name='Field')
    df = df.groupby(['Age Group', 'State', 'Field']).sum().reset_index()
    df.to_csv('Final_Data/ETL/state_agegroup_degree_majors.csv', index = False)
    return df

def get_ungrouped_regional_salaries():
    reg_salaries = pd.read_csv('Datasets/Education Salaries/salaries-by-region.csv')
    reg_salaries['Region'] = reg_salaries.apply(lambda x: set_region(x['Region']), axis=1)
    return reg_salaries


def set_region(reg):
    if(reg == 'California'):
        return 'Western'
    else:
        return reg
    
####Degrees

def get_degree_counts():
    title_maj = pd.read_csv('Datasets/PeopleDataLabs/pdl_job_titles_by_major_edited.csv')
    totals = title_maj[['major','count']]
    totals['title'] = 'total' 
    job1 = _create_jobs(title_maj, 1)
    job2 = _create_jobs(title_maj, 2)
    job3 = _create_jobs(title_maj, 3)
    job4 = _create_jobs(title_maj, 4)
    job5 = _create_jobs(title_maj, 5)
    job6 = _create_jobs(title_maj, 6)
    job7 = _create_jobs(title_maj, 7)
    job8 = _create_jobs(title_maj, 8)
    job9 = _create_jobs(title_maj, 9)
    job10 = _create_jobs(title_maj, 10)
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
    job_list_grp = job_list_grp[['major','category', 'title', 'count']]
    job_list_grp.to_csv('Final_Data/ETL/degree_to_job_title_count.csv', index = False)
    return job_list_grp

def _create_jobs(df, val):
    title = 'job_title_' + str(val)
    count = 'job_title_'+str(val)+'_count'
    job = df[['major','category',title, count]]
    job.columns = ['major','category', 'title', 'count']
    return job

def get_degrees_pay_back():
    df = pd.read_csv('Datasets/Education Salaries/degrees-that-pay-back_edited.csv' )
    df = df[['Undergraduate Major', 'Category', 'Starting Median Salary',
       'Mid-Career Median Salary']]
    return df

#### Salary

def get_glassdoor_best_jobs():
    g_df = pd.read_csv('Final_Data/Manually Altered/glassdoor_best_jobs.csv')
    g_df = g_df[(g_df['year'] == 2017) |(g_df['year'] == 2020)] 
    return g_df

def get_min_wage():
    min_df = pd.read_csv('Datasets/Education Salaries/MinimumWage.csv')
    return min_df.tail(5)

def get_demogaphics():
    rs_df = pd.read_csv('Datasets/Education Salaries/real_estate_db.csv')
    rs_df = rs_df[['state','state_ab', 'city','pop', 'debt', 'male_pop', 'female_pop', 'rent_mean',
               'rent_median','family_mean', 'family_median', 
               'hc_mortgage_mean', 'hc_mortgage_median', 'hc_mean','hc_median',
               'hs_degree','hs_degree_male', 'hs_degree_female', 'male_age_mean',
               'male_age_median','female_age_mean', 'female_age_median', 'hi_mean', 'hi_median',
               'pct_own', 'married', 'married_snp', 'separated', 'divorced','rent_gt_10', 'rent_gt_15', 'rent_gt_20', 'rent_gt_25',
       'rent_gt_30', 'rent_gt_35', 'rent_gt_40', 'rent_gt_50', 'home_equity']]
    
    rs_df = rs_df.dropna()
    
    rs_grp = rs_df.groupby('state').mean()
    rs_grp.to_csv('Final_Data/ETL/state_demographics.csv')
    return rs_grp
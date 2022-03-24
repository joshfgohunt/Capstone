import pandas as pd
# set desired cols

# get subset of data with cols

# convert column names


def get_subset(fpath, column_subset):
    pd_data = pd.read_csv(fpath, usecols=column_subset)
    return pd_data
#####Rent
def get_rent_df():
    cols = ["SizeRank", "RegionName", "State", "Metro", "CountyName", 
                    "2011-01", "2016-01", "2019-12"]
    df1 = get_subset('Datasets/House Price/City_MedianRentalPrice_1Bedroom.csv', cols);
    df2 = get_subset('Datasets/House Price/City_MedianRentalPrice_2Bedroom.csv', cols);
    df3 = get_subset('Datasets/House Price/City_MedianRentalPrice_3Bedroom.csv', cols);
    df4 = get_subset('Datasets/House Price/City_MedianRentalPrice_4Bedroom.csv', cols);
    df1['size'] = '1br'
    df2['size'] = '2br'
    df3['size'] = '3br'
    df4['size'] = '4br'
    
    merged_df = df1.append(df2, ignore_index = True)
    merged_df = merged_df.append(df3, ignore_index = True)
    merged_df = merged_df.append(df4, ignore_index = True)
    
    changecols = {"2011-01": "2011",  "2016-01": "2016",  "2019-12": "2020"}
    
    merged_df = merged_df.rename(columns = changecols)

    idvars = ['RegionName', 'State', 'Metro', 'CountyName', 'SizeRank', 'size']
    merged_rent_df = merged_df.melt(id_vars=idvars, var_name='Year')

    merged_rent_df = merged_rent_df.dropna(subset=['value'])
    return merged_rent_df

#####ZHVI
def get_zhvi_array():
    cols = [ "RegionName", "RegionType",
                    "StateName", "State", "Metro", "CountyName", "2005-01-31", 
                    "2011-01-31", "2016-01-31", "2020-01-31"]
    df1 = get_subset('Datasets/House Price/City_Zhvi_1Bedroom.csv', cols);
    df2 = get_subset('Datasets/House Price/City_Zhvi_2Bedroom.csv', cols);
    df3 = get_subset('Datasets/House Price/City_Zhvi_3Bedroom.csv', cols);
    df4 = get_subset('Datasets/House Price/City_Zhvi_4Bedroom.csv', cols);
    df1['size'] = '1br'
    df2['size'] = '2br'
    df3['size'] = '3br'
    df4['size'] = '4br'

    merged_df = df1.append(df2, ignore_index = True)
    merged_df = merged_df.append(df3, ignore_index = True)
    merged_df = merged_df.append(df4, ignore_index = True)

    changecols = {"2005-01-31": "2005","2011-01-31": "2011",  "2016-01-31": "2016",  "2020-01-31": "2020"}
    merged_df = merged_df.rename(columns = changecols)

    idvars = ["RegionName", "RegionType",
                    "StateName", "State", "Metro", "CountyName", 'size']
    merged_zhvi_df = merged_df.melt(id_vars=idvars, var_name='Year')
    
    merged_zhvi_df = merged_zhvi_df.dropna(subset=['value'])
    return merged_zhvi_df

#####Sales Data
def get_city_sales_price():
    city_sales_price = pd.read_csv('Datasets/House Price/Sale_Prices_City.csv')
    city_sales_price = city_sales_price[['RegionName', 'StateName', 
        '2009-01','2010-01', '2011-01', '2012-01', '2013-01', '2014-01',
        '2015-01', '2016-01', '2017-01', '2018-01', '2019-01', '2020-01']]
    changecols = {'RegionName': 'Region', 'StateName': 'State', 
        '2009-01': '2009','2010-01': '2010', '2011-01' : '2011', '2012-01': '2012', '2013-01': '2013', '2014-01': '2014',
        '2015-01': '2015', '2016-01': '2016', '2017-01': '2017', '2018-01': '2018', '2019-01': '2019', '2020-01': '2020'}
    city_sales_price = city_sales_price.rename(columns = changecols)

    idvars = ['Region', 'State']
    city_sales_price = city_sales_price.melt(id_vars=idvars, var_name='Year')
    city_sales_price = city_sales_price.dropna(subset = ['value'])
    return city_sales_price

def get_state_sales_price():
    state_sales_price = pd.read_csv('Datasets/House Price/Sale_Prices_State.csv')
    state_sales_price = state_sales_price[[ 'RegionName', 
        '2009-01','2010-01', '2011-01', '2012-01', '2013-01', '2014-01',
        '2015-01', '2016-01', '2017-01', '2018-01', '2019-01', '2020-01']]
    changecols = {'RegionName': 'State', 
        '2009-01': '2009','2010-01': '2010', '2011-01' : '2011', '2012-01': '2012', '2013-01': '2013', '2014-01': '2014',
        '2015-01': '2015', '2016-01': '2016', '2017-01': '2017', '2018-01': '2018', '2019-01': '2019', '2020-01': '2020'}
    state_sales_price = state_sales_price.rename(columns = changecols)

    idvars = ['State']
    state_sales_price = state_sales_price.melt(id_vars=idvars, var_name='Year')
    state_sales_price = state_sales_price.dropna(subset = ['value'])
    return state_sales_price
def get_city_days():
    city_days = pd.read_csv('Datasets/House Price/DaysOnZillow_City.csv')
    city_days = city_days[['RegionName', 'StateName', 
       '2010-01', '2011-01', '2012-01', '2013-01', '2014-01',
        '2015-01', '2016-01', '2017-01', '2018-01', '2019-01', '2020-01']]
    changecols = {'RegionName': 'Region','StateName': 'State', 
        '2010-01': '2010', '2011-01' : '2011', '2012-01': '2012', '2013-01': '2013', '2014-01': '2014',
        '2015-01': '2015', '2016-01': '2016', '2017-01': '2017', '2018-01': '2018', '2019-01': '2019', '2020-01': '2020'}
    city_days = city_days.rename(columns = changecols)

    idvars = ['Region','State']
    city_days = city_days.melt(id_vars=idvars, var_name='Year')
    city_days = city_days.dropna(subset = ['value'])
    return city_days

def get_state_days():
    state_days = pd.read_csv('Datasets/House Price/DaysOnZillow_State.csv')
    state_days = state_days[['RegionName',
       '2010-01', '2011-01', '2012-01', '2013-01', '2014-01',
        '2015-01', '2016-01', '2017-01', '2018-01', '2019-01', '2020-01']]
    changecols = {'RegionName': 'State', 
        '2010-01': '2010', '2011-01' : '2011', '2012-01': '2012', '2013-01': '2013', '2014-01': '2014',
        '2015-01': '2015', '2016-01': '2016', '2017-01': '2017', '2018-01': '2018', '2019-01': '2019', '2020-01': '2020'}
    state_days = state_days.rename(columns = changecols)

    idvars = ['State']
    state_days = state_days.melt(id_vars=idvars, var_name='Year')
    state_days = state_days.dropna(subset = ['value'])
    return state_days

#####Regional Population
def get_population_data():
    cols_1 = ['NAME', 'STATENAME',
       'POP_2000', 'POP_2001', 'POP_2002',
       'POP_2003', 'POP_2004', 'POP_2005']
    cols_2 = ['NAME', 'STNAME', 'POPESTIMATE2010', 'POPESTIMATE2011',
       'POPESTIMATE2012', 'POPESTIMATE2013', 'POPESTIMATE2014',
       'POPESTIMATE2015', 'POPESTIMATE2016', 'POPESTIMATE2017',
       'POPESTIMATE2018', 'POPESTIMATE2019', 'POPESTIMATE2020']
    df1 = get_subset('Datasets/Population/2005_Us_Population_By_City.csv', cols_1);
    df2 = get_subset('Datasets/Population/2020_Us_Population_By_City.csv', cols_2);
   
    changecols_1 = {'NAME': 'Region', 'STATENAME': 'State'}
    
    changecols_2 = {'NAME': 'Region', 'STNAME': 'State', 'POPESTIMATE2010': 'POP_2010', 'POPESTIMATE2011': 'POP_2011',
       'POPESTIMATE2012': 'POP_2012', 'POPESTIMATE2013': 'POP_2013', 'POPESTIMATE2014': 'POP_2014',
       'POPESTIMATE2015': 'POP_2015', 'POPESTIMATE2016': 'POP_2016', 'POPESTIMATE2017': 'POP_2017',
       'POPESTIMATE2018': 'POP_2018', 'POPESTIMATE2019': 'POP_2019', 'POPESTIMATE2020': 'POP_2020'}
     
    df1 = df1.rename(columns = changecols_1)
    df2 = df2.rename(columns = changecols_2)
    merged_df = df1.merge(df2, left_on=['Region','State'],right_on=['Region','State'], how='inner').drop_duplicates().reset_index(drop=True)
    
    merged_df['Mean_Pop'] = merged_df.iloc[:,3:19].mean(axis=1)
    merged_df.to_csv('Datasets/Population/Merged_Pop_Data.csv')
    return merged_df

def get_cleaned_city_pop_data():
    df = get_population_data()
    reg_df = df[df['Region'] != df['State']]
    reg_df = reg_df[reg_df['Mean_Pop'] > 0]
    return reg_df

def get_cleaned_state_pop_data():
    df = get_population_data()
    state_df = df[df['Region'] == df['State']]
    return state_df

################Regional Degrees
def get_bachelors_degree_majors():
    cols = [ 'Sex', 'Age Group','State', 'Bachelor\'s Degree Holders',
       'Science and Engineering', 'Science and Engineering Related Fields',
       'Business', 'Education', 'Arts, Humanities and Others']
    df = get_subset('Datasets/Regional Education/Bachelor_Degree_Majors.csv', cols)
    
    fields = ['Bachelor\'s Degree Holders', 'Science and Engineering','Science and Engineering Related Fields','Business','Education','Arts, Humanities and Others']
    df[fields] = df[fields].apply(pd.to_numeric)
    df = df[df['Sex'] !='Total']
    return df

############## Degree Counts
def get_degree_counts():
    title_maj = pd.read_csv('Datasets/PeopleDataLabs/pdl_job_titles_by_major.csv')
    sub = pd.read_csv('Datasets/PeopleDataLabs/degree_subcategory_lookup.csv')
    merge = sub.merge(title_maj, left_on='major', right_on='major')
    merge = merge[merge['category'] != 'other']
    return merge
#####Zillow
def get_days_on_zillow_city():
    cols = ["SizeRank", "RegionID", "RegionName", "RegionType", "StateName","2011-01", "2016-01", "2020-01" ]
    df = get_subset('Datasets/House Price/DaysOnZillow_City.csv', cols)
    # changecols = {"2011-01": "2011",  "2016-01": "2016",  "2020-01": "2020", "StateName: State"}
    return df

def get_days_on_zillow_state():
    cols = ["SizeRank", "RegionID", "RegionName", "RegionType","2011-01", "2016-01", "2020-01" ]
    df = get_subset('Datasets/House Price/DaysOnZillow_State.csv', cols)
    # changecols = {"2011-01": "2011",  "2016-01": "2016",  "2020-01": "2020"}
    return df

####Degrees
def get_placement_data():
    cols = ['hsc_s', 'degree_t', 'workex', 'etest_p', 'specialisation','status', 'salary' ]
    df = get_subset('Datasets/Education Salaries/Placement_Data_Full_Class.csv', cols)
    changecols = {"hsc_s": "early_specialisation",  "degree_t": "degree"}
    df = df.rename(columns = changecols)
    return df


#Not used yet
def get_degrees_pay_back():
    fpath = "Datasets/Salaries/degrees-that-pay-back.csv"
    df = pd.read_csv(fpath)
    return df

#Not used yet
def get_min_wage():
    fpath = "Datasets/Salaries/MinimumWage.csv"
    df = pd.read_csv(fpath)
    return df

#Not used yet
def get_salaries_college_type():
    fpath = "Datasets/Salaries/salaries-by-college-type.csv"
    df = pd.read_csv(fpath)
    return df

#Not used yet
def get_salaries_region():
    fpath = "Datasets/Salaries/salaries-by-region.csv"
    df = pd.read_csv(fpath)
    return df


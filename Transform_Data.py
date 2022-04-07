import pandas as pd


########### Add State Info ################
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
'West Virginia': 'WV', 'Wisconsin': 'WI', 'Wyoming': 'WY', 'District of Columbia' : 'DC'}
    
    us_abbr = pd.DataFrame.from_dict(us_state_abbrev, orient='index')
    us_abbr = us_abbr.reset_index()
    us_abbr.columns = ['State', 'Abbr'] 
    right = 'State'
    df = df.merge(us_abbr, how='inner', left_on=left, right_on=right)
    return df

def add_state_region(df, left):
    state_region = {'Alabama': 'Southern', 'Alaska': 'Western', 'Arizona': 'Western', 'Arkansas': 'Southern', 'California': 'Western', 'Colorado': 'Western',
'Connecticut': 'Northeastern', 'Delaware': 'Southern', 'Florida': 'Southern', 'Georgia': 'Southern', 'Hawaii': 'Western', 'Idaho': 'Western',
'Illinois': 'Midwestern', 'Indiana': 'Midwestern', 'Iowa': 'Midwestern', 'Kansas': 'Midwestern', 'Kentucky': 'Southern', 'Louisiana': 'Southern',
'Maine': 'Northeastern', 'Maryland': 'Southern', 'Massachusetts': 'Northeastern', 'Michigan': 'Midwestern', 'Minnesota': 'Midwestern', 'Mississippi': 'Southern',
'Missouri': 'Midwestern', 'Montana': 'Western', 'Nebraska': 'Midwestern', 'Nevada': 'Western', 'New Hampshire': 'Northeastern', 'New Jersey': 'Northeastern',
'New Mexico': 'Western', 'New York': 'Northeastern', 'North Carolina': 'Southern', 'North Dakota': 'Midwestern', 'Ohio': 'Midwestern', 'Oklahoma': 'Southern',
'Oregon': 'Western', 'Pennsylvania': 'Northeastern', 'Rhode Island': 'Northeastern', 'South Carolina': 'Southern', 'South Dakota': 'Midwestern',
'Tennessee': 'Southern', 'Texas': 'Southern', 'Utah': 'Western', 'Vermont': 'Northeastern', 'Virginia': 'Southern', 'Washington': 'Western',
'West Virginia': 'Southern', 'Wisconsin': 'Midwestern', 'Wyoming': 'Western', 'District of Columbia' : 'Southern'}
    state_region = pd.DataFrame.from_dict(state_region, orient='index')
    state_region = state_region.reset_index()
    state_region.columns = ['State', 'Region'] 
    right = 'State'
    df = df.merge(state_region, how='outer', left_on=left, right_on=right)
    return df



########### Consolidating Data ##########


### Location

def consolidate_sell_pop(location_house_sell_time, location_state_pop):
    location_house_sell_time.columns = ['state', 'year', 'days_to_sell']
    location_state_pop.columns = ['state', 'year', 'population']
    merged_loc = location_house_sell_time.merge(location_state_pop, left_on= ['state', 'year'], right_on= ['state', 'year'], how='inner')
    return merged_loc


def consolidate_sale_rent(location_rental_prices, location_house_prices):
    location_rental_prices.columns = ['state', 'size', 'year', 'rent_value']
    location_house_prices.columns = ['state', 'size', 'year', 'sell_value']
    housing_merged_loc = location_rental_prices.merge(location_house_prices, left_on= ['state', 'size', 'year'], right_on= ['state', 'size', 'year'], how='inner')
    return housing_merged_loc


def group_state_degree_data(df):
    loc_field_focus = df.groupby(['State','Field'])['value'].sum().reset_index()
    loc_field_focus_totals = df.groupby(['State'])['value'].sum().reset_index()
    loc_field_focus_totals['Field'] = 'Total'
    state_ratio = loc_field_focus.append(loc_field_focus_totals)
    final =state_ratio.pivot_table(index = 'State', columns = 'Field', values = 'value') 
    final = append_zscores(final, 'Total', 'Total_z')
    return final

def group_age_degree_data(df):
    loc_age_focus = df.groupby(['Age Group','Field'])['value'].sum().reset_index()
    loc_age_totals = df.groupby(['Age Group'])['value'].sum().reset_index()
    loc_age_totals['Field'] = 'Total'
    age_ratio = loc_age_focus.append(loc_age_totals)
    final =age_ratio.pivot_table(index = 'Age Group', columns = 'Field', values = 'value') 
    final = append_zscores(final, 'Total', 'Total_z')
    return final

def get_rent_sale_growth():
    location_rental_prices = pd.read_csv('Final_Data/ETL/zillow_rental_prices.csv') 
    location_house_prices = pd.read_csv('Final_Data/ETL/zillow_house_prices.csv') 
    housing_merged_loc = consolidate_sale_rent(location_rental_prices, location_house_prices)
    h_m_17 = housing_merged_loc[housing_merged_loc['year'] == 2017]
    h_m_20 = housing_merged_loc[housing_merged_loc['year'] == 2020]
    h_m_17 = h_m_17[['state','size','rent_value', 'sell_value']]
    h_m_20 = h_m_20[['state','size','rent_value', 'sell_value']]
    h_m_17_1 = h_m_17[h_m_17['size'] == '1br']
    h_m_17_2 = h_m_17[h_m_17['size'] == '2br']
    h_m_17_3 = h_m_17[h_m_17['size'] == '3br']
    h_m_17_4 = h_m_17[h_m_17['size'] == '4br']
    h_m_20_1 = h_m_20[h_m_20['size'] == '1br']
    h_m_20_2 = h_m_20[h_m_20['size'] == '2br']
    h_m_20_3 = h_m_20[h_m_20['size'] == '3br']
    h_m_20_4 = h_m_20[h_m_20['size'] == '4br']
    h_m_17_1 = h_m_17_1[['state', 'rent_value', 'sell_value']]
    h_m_17_2 = h_m_17_2[['state', 'rent_value', 'sell_value']]
    h_m_17_3 = h_m_17_3[['state', 'rent_value', 'sell_value']]
    h_m_17_4 = h_m_17_4[['state', 'rent_value', 'sell_value']]

    h_m_20_1 = h_m_20_1[['state', 'rent_value', 'sell_value']]
    h_m_20_2 = h_m_20_2[['state', 'rent_value', 'sell_value']]
    h_m_20_3 = h_m_20_3[['state', 'rent_value', 'sell_value']]
    h_m_20_4 = h_m_20_4[['state', 'rent_value', 'sell_value']]

    h_m_17_1.columns = ['state', 'rent_value_17_1', 'sell_value_17_1']
    h_m_17_2.columns = ['state', 'rent_value_17_2', 'sell_value_17_2']
    h_m_17_3.columns = ['state', 'rent_value_17_3', 'sell_value_17_3']
    h_m_17_4.columns = ['state', 'rent_value_17_4', 'sell_value_17_4']

    h_m_20_1.columns = ['state', 'rent_value_20_1', 'sell_value_20_1']
    h_m_20_2.columns = ['state', 'rent_value_20_2', 'sell_value_20_2']
    h_m_20_3.columns = ['state', 'rent_value_20_3', 'sell_value_20_3']
    h_m_20_4.columns = ['state', 'rent_value_20_4', 'sell_value_20_4']

    merged_rent_sale = h_m_17_1.merge(h_m_17_2, on='state', how='outer')
    merged_rent_sale = merged_rent_sale.merge(h_m_17_3, on='state', how='outer')
    merged_rent_sale = merged_rent_sale.merge(h_m_17_4, on='state', how='outer')
    merged_rent_sale = merged_rent_sale.merge(h_m_20_1, on='state', how='outer')
    merged_rent_sale = merged_rent_sale.merge(h_m_20_2, on='state', how='outer')
    merged_rent_sale = merged_rent_sale.merge(h_m_20_3, on='state', how='outer')
    merged_rent_sale = merged_rent_sale.merge(h_m_20_4, on='state', how='outer')
    return merged_rent_sale


#### Degree

def combine_demand(education_industry_counts, education_deg_to_job, education_deg_payback,location_state_sex_deg ):
    industry_demand = education_industry_counts.groupby('category')['Count'].sum().to_frame()
    degree_fill_count = education_deg_to_job.groupby('category')['count'].sum().to_frame()
    degree_mean = education_deg_payback.groupby('Category').mean()
    degrees_completed = location_state_sex_deg.groupby('Field')['value'].sum().to_frame()
    
    industry_demand = industry_demand.reset_index()
    industry_demand.columns = ['Field', 'Demand_Count']
    degree_fill_count = degree_fill_count.reset_index()
    degree_fill_count.columns = ['Field', 'Degree_Fill_Count']
    degree_mean = degree_mean.reset_index()
    degree_mean.columns = ['Field', 'start_salary', 'mid_salary']
    degrees_completed = degrees_completed.reset_index()
    degrees_completed.columns = ['Field', 'bachelor_count']
    
    dfs = industry_demand.merge(degree_fill_count, on='Field', how='inner')
    dfs = dfs.merge(degree_mean, on='Field', how='inner')
    dfs = dfs.merge(degrees_completed, on='Field', how='inner')
    return dfs


def append_zscores(df, col, newcol):
    df[newcol] = (df[col] - df[col].mean())/df[col].std()
    return df


#### Education


def get_regional_salaries(reg_salaries):
    reg_salaries = reg_salaries.groupby('Region').mean()
    return reg_salaries

def get_bachelor_ratios(bachelor_counts):
    bachelor_ratio = bachelor_counts.copy()
    bachelor_ratio['Arts, Humanities and Others'] = bachelor_ratio['Arts, Humanities and Others']/bachelor_ratio['Total']
    bachelor_ratio['Business'] = bachelor_ratio['Business']/bachelor_ratio['Total']
    bachelor_ratio['Education'] = bachelor_ratio['Education']/bachelor_ratio['Total']
    bachelor_ratio['Sci_Eng_Related'] = bachelor_ratio['Sci_Eng_Related']/bachelor_ratio['Total']
    bachelor_ratio['Science and Engineering'] = bachelor_ratio['Science and Engineering']/bachelor_ratio['Total']
    return bachelor_ratio
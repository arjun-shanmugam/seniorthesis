#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import geopandas as gpd
from dask.distributed import Client
import dask.dataframe as dd
from dask_jobqueue import SLURMCluster
import os
import matplotlib.pyplot as plt
import constants
import numpy as np
from panel_utilities import get_value_variable_names
import dask_geopandas
INPUT_DATA_EVICTIONS = "../data/02_intermediate/evictions.csv"
INPUT_DATA_OFFENSE_CODES = "../data/01_raw/rmsoffensecodes.xlsx"
INPUT_DATA_TRACTS = "../data/02_intermediate/tracts.csv"
INPUT_DATA_CRIME = "../data/01_raw/crime_incidents"
INPUT_DATA_PERMITS = "../data/01_raw/permits/6ddcd912-32a0-43df-9908-63574f8c7e77.csv"
OUTPUT_DATA_CRIME = "../data/03_cleaned/crime_analysis_monthly.csv"
OUTPUT_DATA_CRIME_FULL_SAMPLE = "../data/03_cleaned/crime_analysis_monthly_full_sample.csv"
OUTPUT_DATA_CRIME_COUNTS = "../data/03_cleaned/crime_counts.csv"
OUTPUT_TABLES = "../output/final_paper/tables/"

VERBOSE = True
N_PARTITIONS = 1
value_vars_to_concat = []  # A list of DataFrames, where each DataFrame contains the panel data for a single outcome variable and has case_number as its index.


# ## 1. Loading Evictions Data

# In[2]:


# Load evictions data.
with open(INPUT_DATA_EVICTIONS, 'r') as file:
    all_column_names = set(file.readline().replace("\"", "").replace("\n", "").split(","))
to_drop = {'Accuracy Score', 'Accuracy Type', 'Number', 'Street', 'Unit Type', 'Unit Number',
           'State', 'Zip', 'Country', 'Source', 'Census Year', 'State FIPS', 'County FIPS',
           'Place Name', 'Place FIPS', 'Census Tract Code', 'Census Block Code', 'Census Block Group',
           'Metro/Micro Statistical Area Code', 'Metro/Micro Statistical Area Type',
           'Combined Statistical Area Code', 'Metropolitan Division Area Code', 'court_location',
           'defendant', 'defendant_atty', 'defendant_atty_address_apt',
           'defendant_atty_address_city', 'defendant_atty_address_name', 'defendant_atty_address_state',
           'defendant_atty_address_street', 'defendant_atty_address_zip', 'docket_history', 'judgment_for',
           'judgment_total', 'plaintiff', 'plaintiff_atty', 'plaintiff_atty_address_apt',
           'plaintiff_atty_address_city', 'plaintiff_atty_address_name', 'plaintiff_atty_address_state',
           'plaintiff_atty_address_street', 'plaintiff_atty_address_zip', 'Metropolitan Division Area Name',
           'property_address_city', 'property_address_state', 'property_address_street',
           'property_address_zip'}
evictions_df = pd.read_csv(INPUT_DATA_EVICTIONS, usecols=set(all_column_names) - set(to_drop))

# Add file month and year to dataset.
evictions_df.loc[:, 'file_month'] = pd.to_datetime(evictions_df['file_date']).dt.strftime('%Y-%m')
evictions_df.loc[:, 'file_year'] = pd.to_datetime(evictions_df['file_date']).dt.year

# Add latest docket month and year to dataset.
evictions_df.loc[:, 'latest_docket_month'] = pd.to_datetime(evictions_df['latest_docket_date']).dt.strftime('%Y-%m')
evictions_df.loc[:, 'latest_docket_year'] = pd.to_datetime(evictions_df['latest_docket_date']).dt.year


# Clean the values in the judgment_for_pdu variable.
judgment_for_pdu_replacement_dict = {"unknown": "Unknown",
                                     "plaintiff": "Plaintiff",
                                     "defendant": "Defendant"}
evictions_df.loc[:, "judgment_for_pdu"] = (evictions_df.loc[:, "judgment_for_pdu"]
                                           .replace(judgment_for_pdu_replacement_dict))

# Replace missing values in money judgment column with zeroes.
evictions_df.loc[:, 'judgment'] = evictions_df['judgment'].fillna(0)

# Rename duration to case_duration.
evictions_df = evictions_df.rename(columns={'duration': 'case_duration'})


# In[3]:


# Restrict sample
sample_restriction_table_index = []
sample_restriction_table_values = []

# Restrict to cases in Boston.
boston_mask = ((evictions_df['County'] == "Suffolk County") & (~evictions_df['City'].isin(["Chelsea", "Revere", "Winthrop"])))
if VERBOSE:
    print(f"Restricting to {boston_mask.sum()} observations which are from Boston.")
evictions_df = evictions_df.loc[boston_mask, :]
original_N = len(evictions_df)
if VERBOSE:
    print(f"Beginning with {original_N} observations.")
sample_restriction_table_index.append("Case Filed in Boston")
sample_restriction_table_values.append(original_N)

# Drop malformed addresses.
if VERBOSE:
    print(f"Dropping {evictions_df['property_address_full'].str.contains('span, span span').sum()} observations which "
          f"have malformed addresses.")
evictions_df = evictions_df.loc[~evictions_df['property_address_full'].str.contains("span, span span"), :]

# Drop missing addresses.
no_address_info_mask = (evictions_df['property_address_full'].isna())
if VERBOSE:
    print(
        f"Dropping {no_address_info_mask.sum()} rows missing property_address_full")
evictions_df = evictions_df.loc[~no_address_info_mask, :]
sample_restriction_table_index.append("Non-missing property address")
sample_restriction_table_values.append(len(evictions_df))
evictions_df_all_boston_cases = evictions_df.copy()

# Drop cases where initiating action is unknown
mask = evictions_df['initiating_action'] == "Summary Process - Residential (c239)"
if VERBOSE:
    print(f"Dropping {mask.sum()} cases for which the initiating action could not be scraped")
evictions_df = evictions_df.loc[~mask, :]
sample_restriction_table_index.append("Non-missing case initiating action")
sample_restriction_table_values.append(len(evictions_df))

# Drop cases initiated due to foreclosure
mask = evictions_df['initiating_action'].str.contains("Foreclosure")
if VERBOSE:
    print(f"Dropping {mask.sum()} cases which began due to foreclosure")
evictions_df = evictions_df.loc[~mask, :]
sample_restriction_table_index.append("Case initiated for reason other than foreclosure")
sample_restriction_table_values.append(len(evictions_df))



# Drop cases missing latest_docket_date.
mask = evictions_df['latest_docket_date'].notna()
if VERBOSE:
    print(
        f"Dropping {evictions_df['latest_docket_date'].isna().sum()} observations where latest_docket_date is missing.")
evictions_df = evictions_df.loc[mask, :]
sample_restriction_table_index.append("Non-missing latest docket date")
sample_restriction_table_values.append(len(evictions_df))



# Drop cases where disposition found is other.
disposition_found_other_mask = (evictions_df['disposition_found'] == "Other") | (evictions_df['disposition_found'].isna())
if VERBOSE:
    print(f"Dropping {disposition_found_other_mask.sum()} cases where disposition_found is \"Other\" or missing.")
evictions_df = evictions_df.loc[~disposition_found_other_mask, :]
sample_restriction_table_index.append("Cases for which disposition could be scraped")
sample_restriction_table_values.append(len(evictions_df))

# Drop cases resolved via mediation.
mediated_mask = evictions_df['disposition_found'] == "Mediated"
if VERBOSE:
    print(f"Dropping {mediated_mask.sum()} cases resolved through mediation.")
evictions_df = evictions_df.loc[~mediated_mask, :]
sample_restriction_table_index.append("Case not resolved through mediation")
sample_restriction_table_values.append(len(evictions_df))


# Drop cases concluded in April 2020 or later.
pre_pandemic_months = ['2019-04',
 '2019-05',
 '2019-06',
 '2019-07',
 '2019-08',
 '2019-09',
 '2019-10',
 '2019-11',
 '2019-12',
 '2020-01',
 '2020-02',
 '2020-03']
pre_pandemic_mask = evictions_df['latest_docket_month'].isin(pre_pandemic_months)
evictions_df = evictions_df.loc[pre_pandemic_mask, :]
if VERBOSE:
    print(f"Dropping {(~pre_pandemic_mask).sum()} cases which concluded after pandemic began")
sample_restriction_table_index.append("Case concluded before April 2020")
sample_restriction_table_values.append(len(evictions_df))


defendant_is_not_entity_mask = evictions_df['isEntityD'] == 0
evictions_df = evictions_df.loc[defendant_is_not_entity_mask, :]
if VERBOSE: 
    print(f"Dropping {(~defendant_is_not_entity_mask).sum()} cases which were filed against defendants who are entities")
sample_restriction_table_index.append("Defendant is an individual and not an entity")
sample_restriction_table_values.append(len(evictions_df))
          


# In[4]:


# Build sample restriction table
sample_restriction_table = pd.DataFrame()
sample_restriction_table["Restriction"] = sample_restriction_table_index
sample_restriction_table["Observations"] = sample_restriction_table_values                         
sample_restriction_table = sample_restriction_table.set_index("Restriction")

# Export to LaTeX.
filename = os.path.join(OUTPUT_TABLES, "sample_restriction.tex")
sample_restriction_table.style.format(formatter="{:,.0f}").to_latex(filename, hrules=True)
sample_restriction_table


# ## 2. Merging Evictions With Census Tract Characteristics

# In[5]:


# Add flags indicating what sample each record corresponds to 
evictions_df_all_boston_cases.loc[:, 'main_analysis_sample'] = 0
evictions_df_all_boston_cases.loc[evictions_df_all_boston_cases['case_number'].isin(evictions_df['case_number']), 'main_analysis_sample'] = 1
evictions_df = evictions_df_all_boston_cases.copy()
mediated_mask = evictions_df['disposition_found'] == "Mediated"
post_pandemic_mask = ~(evictions_df['latest_docket_month'].isin(pre_pandemic_months))
evictions_df.loc[:, 'mediated_sample'] = 0
evictions_df.loc[mediated_mask, 'mediated_sample'] = 1
evictions_df.loc[:, 'covid_sample'] = 0
evictions_df.loc[post_pandemic_mask, 'covid_sample'] = 1




# Merge with census tract characteristics.
evictions_df = evictions_df.rename(columns={'Full FIPS (tract)': 'tract_geoid'})
evictions_tracts_df = evictions_df.merge(pd.read_csv(INPUT_DATA_TRACTS, dtype={'tract_geoid': float}),
                                  on='tract_geoid',
                                  how='left',
                                  validate='m:1').set_index('case_number')
if VERBOSE:
    print(f"Successfully merged {evictions_tracts_df['med_hhinc2016'].notna().sum()} observations with census tracts.")


# ## 3. Merging Evictions with Boston Neighborhoods

# In[6]:


# Create a GeoSeries containing eviction Points as its geometry, case_number as its index, and no other columns.
evictions_gdf = gpd.GeoDataFrame(evictions_df, geometry=gpd.points_from_xy(evictions_df['Longitude'], evictions_df['Latitude']))[['geometry', 'case_number']]
evictions_gdf = evictions_gdf.set_crs("EPSG:4326", allow_override=True).to_crs('EPSG:26986')


# ## 4. Merge Evictions with Crimes

# In[7]:


# Request computing resources.
cluster = SLURMCluster(queue='batch',
                       cores=32,
                       memory='230 GB',
                       walltime='00:20:00',
                      scheduler_options={'dashboard_address': '8787'} )
cluster.scale(jobs=1)


# In[8]:


client = Client(cluster)


# In[9]:


# Get property crime and violent crime offense codes
offense_codes_df = pd.read_excel(INPUT_DATA_OFFENSE_CODES, usecols=['CODE', 'property_crime', 'violent_crime'])
property_crime_offense_codes = offense_codes_df.loc[offense_codes_df['property_crime'] == 1, 'CODE'].unique().tolist()
non_property_crime_offense_codes = offense_codes_df.loc[offense_codes_df['property_crime'] == 0, 'CODE'].unique().tolist()
violent_crime_offense_codes = offense_codes_df.loc[offense_codes_df['violent_crime'] == 1, 'CODE'].unique().tolist()
non_violent_crime_offense_codes = offense_codes_df.loc[offense_codes_df['violent_crime'] == 0, 'CODE'].unique().tolist()


# In[10]:


# Read crime data as Dask DataFrame, then compute back to DataFrame.
crime_df = (dd.read_csv(INPUT_DATA_CRIME + "/*.csv", dtype={'REPORTING_AREA': 'object', 'SHOOTING': 'object'})
                .dropna(subset=['Long', 'Lat', 'OCCURRED_ON_DATE'])  # Drop crimes missing latitude, longitude, or date, as they cannot be merged with panel.
                .rename(columns={'OCCURRED_ON_DATE': 'month_of_crime_incident'})
                .drop(columns=['OFFENSE_CODE_GROUP', 'OFFENSE_DESCRIPTION', 'DISTRICT', 'REPORTING_AREA', 'YEAR', 'MONTH',
                               'DAY_OF_WEEK', 'HOUR', 'UCR_PART', 'STREET', 'Location'])  # Drop unneeded columns
                .compute())
                # Must call compute here and then briefly convert back to in-memory dataset because dask_geopandas.points_from_xy not working.
# Keep track of the month of crime incident in YYYY-MM format.
crime_df.loc[:, 'month_of_crime_incident'] = pd.to_datetime(crime_df['month_of_crime_incident'].str[:10]).dt.to_period("M").astype(str)
# Convert DataFrame to GeoDataFrame.
crime_gdf = gpd.GeoDataFrame(crime_df, geometry=gpd.points_from_xy(crime_df['Long'], crime_df['Lat']))
crime_gdf = crime_gdf.set_crs("EPSG:4326", allow_override=True).to_crs("EPSG:26986")  # Convert to the correct CRS.
# Convert GeoDataFrame to Dask GeoDataFrame.
crime_dgdf = dask_geopandas.from_geopandas(crime_gdf, npartitions=N_PARTITIONS).repartition(partition_size='5 MB')

columns_for_each_outcome = []
offense_groups = ['all',
                  violent_crime_offense_codes,
                  non_violent_crime_offense_codes,
                  constants.Analysis.trespassing]
radii = list(range(200, 460, 10)) + ["250_to_300", "250_to_350", "250_to_400"]
for offense_group_num, offense_group in enumerate(offense_groups):
    
    for radius in radii:
        
        if isinstance(radius, int): 
            # Create a new GeoDataFrame with geometry equal to circles around each eviction with the current radius.
            current_evictions_gdf = evictions_gdf.copy()
            current_evictions_gdf.geometry = current_evictions_gdf.geometry.buffer(radius)
            current_evictions_dgdf = dask_geopandas.from_geopandas(current_evictions_gdf, npartitions=N_PARTITIONS).repartition(partition_size='5 MB')
        else: 
            # We need to create a "donut" around the property
            inner_radius = evictions_gdf.geometry.buffer(int(radius[:3]))
            outer_radius = evictions_gdf.geometry.buffer(int(radius[-3:]))
            donut = outer_radius.difference(inner_radius)
            current_evictions_gdf = evictions_gdf.copy()
            current_evictions_gdf.geometry = donut
            current_evictions_dgdf = dask_geopandas.from_geopandas(current_evictions_gdf, npartitions=N_PARTITIONS).repartition(partition_size='5 MB')

    
        # Merge evictions with crimes that fall into radius.
        current_evictions_crime_dgdf = dask_geopandas.sjoin(crime_dgdf, current_evictions_dgdf, how='inner', predicate='within')
        current_evictions_crime_dgdf = current_evictions_crime_dgdf.drop(columns=['geometry','index_right'])
        
        if radius == 250 and offense_group == "all":
            # Create a unique list of all crimes which occurred within 250m of any property in our dataset
            (current_evictions_crime_dgdf[['INCIDENT_NUMBER', 'OFFENSE_CODE']]
             .drop_duplicates()
             .set_index('OFFENSE_CODE')
             .join(offense_codes_df[['CODE', 'violent_crime']]
                   .drop_duplicates()
                   .rename(columns={'CODE': 'OFFENSE_CODE'})
                   .set_index('OFFENSE_CODE'))
             .reset_index()
             .groupby(['OFFENSE_CODE', 'violent_crime'], dropna=False)
             .count()
             .compute()
             .to_csv(OUTPUT_DATA_CRIME_COUNTS))
        
        current_evictions_crime_dgdf['INCIDENT_NUMBER'] = 1  # Replace column with 1s so we can count crimes using sum function.
        if offense_group != 'all':  # If we are summing a specific subcategory of crimes
            mask = current_evictions_crime_dgdf['OFFENSE_CODE'].isin(offense_group)
            # Multiply mask by 'INCIDENT_NUMBER' to zero out crimes in different offense groups.
            current_evictions_crime_dgdf['INCIDENT_NUMBER'] = current_evictions_crime_dgdf['INCIDENT_NUMBER'] * mask

                
            
        # Aggregate crimes to case-month level.
        current_panel_long = (current_evictions_crime_dgdf
                              .groupby(['case_number', 'month_of_crime_incident'])
                              .aggregate({'INCIDENT_NUMBER': 'sum'})
                              .reset_index()
                              .rename(columns={'INCIDENT_NUMBER': 'crime_incidents'})
                              .compute())

        current_panel_wide = pd.pivot(current_panel_long, index=['case_number'], columns=['month_of_crime_incident'],
                                      values='crime_incidents').reset_index().set_index('case_number')
        current_panel_wide.columns = [f'{column}_group_{offense_group_num}_crimes_{radius}m' for column in current_panel_wide.columns]
        columns_for_each_outcome.append(current_panel_wide.columns)
        value_vars_to_concat.append(dd.from_pandas(current_panel_wide, npartitions=N_PARTITIONS))


# In[ ]:


# Read permits data as Dask DataFrame, then compute back to DataFrame
permits_df = (pd.read_csv(INPUT_DATA_PERMITS,
                          dtype={'parcel_id': 'float64',
                                                                'property_id': 'float64',
                                                                'sq_feet': 'float64',
                                                                'zip': 'object'}, 
                          usecols=['occupancytype', 'gpsy', 'gpsx', 'issued_date', 'expiration_date', 'declared_valuation'])
              .dropna(subset=['occupancytype', 'gpsy', 'gpsx', 'issued_date', 'expiration_date']))
print("read data")

# Restrict to rows not for commercial development
permits_df = permits_df.loc[(permits_df['occupancytype'] != "Comm") & (permits_df['occupancytype'] != "COMM"), :]

# Convert declared valuation column to numeric
permits_df.loc[:, 'declared_valuation'] = (permits_df['declared_valuation']
                                           .str.replace("$", "", regex=False)
                                           .str.replace(",", "", regex=False).astype(float))
# Drop rows where expiration date is before issue date
permits_df = permits_df.loc[(permits_df['issued_date']) <= (permits_df['expiration_date']), :] 
print("limited to correct rows")

# Explode so that each permit-month has its own row
permits_df.loc[:, 'issued_date'] = pd.to_datetime(permits_df['issued_date'])
# Add one month to expiration dates to ensure that the month in which the permit expires is included in the list of intervening months
permits_df.loc[:, 'expiration_date'] = pd.to_datetime(permits_df['expiration_date']) + pd.DateOffset(months=1)
intervening_months_list = []
for index, row in permits_df.iterrows():
    calculate_dates = pd.date_range(row['issued_date'], 
                                    row['expiration_date'], 
                                    freq='M')
    dates_series = pd.to_datetime(pd.Series(calculate_dates .format()).str[:10]).dt.to_period("M").astype(str)
    intervening_months_list.append(dates_series)
permits_df.loc[:, 'month'] = intervening_months_list
# Drop unneeded rows before exploding
permits_df = permits_df.drop(columns=['issued_date', 'expiration_date', 'occupancytype'])
permits_df = permits_df.explode('month') 
print("exploded")

# Create GeoDataFrame
permits_gdf = gpd.GeoDataFrame(permits_df, geometry=gpd.points_from_xy(permits_df['gpsx'], permits_df['gpsy']))
permits_gdf = permits_gdf.set_crs("EPSG:2249", allow_override=True).to_crs("EPSG:26986")  # Convert to the correct CRS.
permits_gdf = permits_gdf.drop(columns=['gpsx', 'gpsy'])

# Set permitnumber to 1 so we can count permits using sum function
permits_gdf.loc[:, 'permitnumber'] = 1
print(permits_gdf.columns)
print(permits_gdf.dtypes)
print(len(permits_gdf))



# Convert GeoDataFrame to Dask GeoDataFrame.
permits_dgdf = dask_geopandas.from_geopandas(permits_gdf, npartitions=N_PARTITIONS).repartition(partition_size='100 MB')

radii = [15, 25, 50, 75, 100] + ["250_to_300", "250_to_350", "250_to_400"]
for radius in radii:
    print(radius)
    if isinstance(radius, int): 
        # Create a new GeoDataFrame with geometry equal to circles around each eviction with the current radius.
        current_evictions_gdf = evictions_gdf.copy()
        current_evictions_gdf.geometry = current_evictions_gdf.geometry.buffer(radius)
        current_evictions_dgdf = dask_geopandas.from_geopandas(current_evictions_gdf, npartitions=N_PARTITIONS).repartition(partition_size='5 MB')
    else: 
        # We need to create a "donut" around the property
        inner_radius = evictions_gdf.geometry.buffer(int(radius[:3]))
        outer_radius = evictions_gdf.geometry.buffer(int(radius[-3:]))
        donut = outer_radius.difference(inner_radius)
        current_evictions_gdf = evictions_gdf.copy()
        current_evictions_gdf.geometry = donut
        current_evictions_dgdf = dask_geopandas.from_geopandas(current_evictions_gdf, npartitions=N_PARTITIONS).repartition(partition_size='5 MB')



    # Merge evictions with permits that fall into radius.
    current_evictions_permits_dgdf = dask_geopandas.sjoin(permits_dgdf, current_evictions_dgdf, how='inner', predicate='within')
    current_evictions_permits_dgdf = current_evictions_permits_dgdf.drop(columns=['geometry','index_right'])

    print("merge finished")


    
    # Aggregate permits to case-month level.
    current_panel_long = (current_evictions_permits_dgdf
                          .groupby(['case_number', 'month'])
                          .aggregate({'permitnumber': 'sum', 'declared_valuation': 'sum'})
                          .reset_index()
                          .rename(columns={'permitnumber': 'permits', 'declared_valuation': 'total_value'})
                          .compute())
    print("aggregated")
    
    # Pivot monthly permit counts
    current_panel_wide_counts = pd.pivot(current_panel_long, index=['case_number'], columns=['month'],
                                  values='permits').reset_index().set_index('case_number')
    print("pivoted")
    current_panel_wide_counts.columns = [f'{column}_permits_{radius}m' for column in current_panel_wide_counts.columns]
     
    
    # Pivot monthly permit valuation means 
    current_panel_wide_means = pd.pivot(current_panel_long, index=['case_number'], columns=['month'], 
                                  values='total_value').reset_index().set_index('case_number')
    current_panel_wide_means.columns = [f'{column}_total_value_{radius}m' for column in current_panel_wide_means.columns]
    print("pivoted")
    
    
    columns_for_each_outcome.append(current_panel_wide_counts.columns)
    columns_for_each_outcome.append(current_panel_wide_means.columns)
    
    value_vars_to_concat.append(dd.from_pandas(current_panel_wide_counts, npartitions=N_PARTITIONS))    
    value_vars_to_concat.append(dd.from_pandas(current_panel_wide_means, npartitions=N_PARTITIONS))


# ## 5. Producing the Analysis Dataset

# In[ ]:


crime_df = dd.multi.concat([dd.from_pandas(evictions_tracts_df, npartitions=N_PARTITIONS)] + value_vars_to_concat, axis=1).compute()
# For evictions not matched to any crimes, fill NA values with 0.
for group_of_columns in columns_for_each_outcome:
    crime_df[group_of_columns] = crime_df[group_of_columns].fillna(0)


# In[ ]:


# Generate a variable indicating judgment in favor of defendant.
crime_df.loc[:, 'judgment_for_defendant'] = 0
defendant_won_mask = ((crime_df['disposition_found'] == "Dismissed") |
                      (crime_df['judgment_for_pdu'] == "Defendant") |
                      (crime_df['disposition'].str.contains('R 41(a)(1) Voluntary Dismissal', regex=False)))
crime_df.loc[defendant_won_mask, 'judgment_for_defendant'] = 1
crime_df.loc[crime_df['main_analysis_sample'] != 1, 'judgment_for_defendant'] = np.nan

# Generate a variable indicating judgement in favor of plaintiff.
crime_df.loc[:, 'judgment_for_plaintiff'] = 1 - crime_df['judgment_for_defendant']


# In[ ]:


# Create variables containing pre-treatment outcomes
outcomes = [f"group_{group_num}_crimes_{radius}m" for group_num in range(len(offense_groups)) for radius in radii]

for outcome in outcomes:
    # Save value variable names and dictionary mapping months to integers
    triplet = get_value_variable_names(crime_df, outcome)
    weekly_value_vars_crime, month_to_int_dictionary, _ = triplet
    
    # Create variable equal to sum of crimes in 2017
    columns_from_2017 = [column for column in crime_df.columns if column.startswith('2017') and column.endswith(outcome)]
    crime_df.loc[:, f'total_twenty_seventeen_{outcome}'] = crime_df[columns_from_2017].sum(axis=1)
    
    # Create variable equal to change in the total number of crimes in the two years prior to treatment
    crime_df_copy = crime_df.copy().reset_index()
    crime_df_copy = pd.melt(crime_df_copy,
                            id_vars=['case_number',
                                     'file_month'],
                            value_vars=weekly_value_vars_crime,
                            var_name='month')
    crime_df_copy.loc[:, 'month'] = crime_df_copy['month'].str[:7]
    crime_df_copy.loc[:, 'calendar_month'] = crime_df_copy['month'].str[:7]
    crime_df_copy.loc[:, ['file_month', 'month']] = crime_df_copy[['file_month', 'month']].replace(month_to_int_dictionary)
    crime_df_copy.loc[:, 'treatment_relative_month'] = crime_df_copy['month'] - crime_df_copy['file_month']

    
    
    crime_df_copy = crime_df_copy.loc[crime_df_copy['treatment_relative_month'].isin([-12, -6, 0]), ['case_number', 'treatment_relative_month', 'value']]
    crime_df_copy = crime_df_copy.pivot(index='case_number', columns='treatment_relative_month', values='value')
    month_neg_twelve = (crime_df_copy[-12]).rename(f'month_neg_twelve_{outcome}')
    month_neg_six = (crime_df_copy[-6]).rename(f'month_neg_six_{outcome}')

    crime_df = pd.concat([crime_df,
                          month_neg_twelve,
                          month_neg_six], axis=1)
    


# In[ ]:


for_cause_mask = crime_df['initiating_action'].isin(["SP Summons and Complaint - Cause",
                                                     "Summary Process - Residential-Cause other than Non payment of rent.",
                                                     "SP Transfer - Cause"])
crime_df.loc[:, 'for_cause'] = np.where(for_cause_mask, 1, 0)

no_cause_mask = crime_df['initiating_action'].isin(["SP Summons and Complaint - No Cause",
                                                     "SP Transfer- No Cause"])
crime_df.loc[:, 'no_cause'] = np.where(no_cause_mask, 1, 0)

non_payment_of_rent_mask = crime_df['initiating_action'].isin(["SP Summons and Complaint - Non-payment of Rent",
                                                               "SP Transfer - Non-payment of Rent"])
crime_df.loc[:, 'non_payment'] = np.where(non_payment_of_rent_mask, 1, 0)


# In[ ]:


# Create case resolution variables
panel_E_columns = ['dismissed', 'defaulted', 'mediated', 'heard']
origin_columns = ['disposition_found', 'disposition_found','disposition_found',
                  'disposition_found']
target_values = ["Dismissed", "Defaulted", "Mediated", "Heard"]


for dummy_column, origin_column, target_value in zip(panel_E_columns, origin_columns, target_values):
    crime_df.loc[:, dummy_column] = np.where(crime_df[origin_column] == target_value, 1, 0)


# In[ ]:


crime_df.loc[crime_df['main_analysis_sample'] == 1, :].to_csv(OUTPUT_DATA_CRIME)
crime_df.to_csv(OUTPUT_DATA_CRIME_FULL_SAMPLE)


import dataikuapi
import dataiku
from dataiku.customrecipe import *
import numpy as np
import pandas as pd
import datetime
import json


### READ INPUT PARAMETERS ###

def read_parameter(num, param):
    param_name = 'instance{}_{}'.format(num, param)
    param_val = get_recipe_config().get(param_name, None)
    # abc
    return param_val

user_limit_per_license = get_recipe_config().get('limit_per_license')
num_instances = int(get_recipe_config().get('num_instances'))

urls = []
api_keys = []

for num in range(1, num_instances+1):
    # read parameter values
    url = read_parameter(num, 'url')
    api_key = read_parameter(num, 'api_key')

    # append to lists
    api_keys.append(api_key)
    urls.append(url)

### GENERATE FULL DATAFRAME ###

# instantiate lists
display_names = []
emails = []
user_groups = []
logins = []
user_profs = []

now = datetime.datetime.now()

full_df = pd.DataFrame(columns=['instance_url','display_name','login','email','user_profile','user_groups'])

for url, api_key in zip(urls, api_keys):
    print url
    client = dataikuapi.DSSClient(url, api_key)

    # get license id
    licensing_status = client.get_licensing_status()
    license_id = licensing_status['base']['licenseContent']['licenseId']

    # get user-specific info
    for user in client.list_users():

        display_name = user['displayName']
        user_groups = user['groups']
        login = user['login']
        user_prof = user['userProfile']
        try:
            email = user['email']
        except:
            email = np.NaN

        full_df = full_df.append({'license_id':license_id,
                                  'instance_url':url,
                                  'display_name':display_name,
                                  'login':login,
                                  'email':email,
                                  'user_profile':user_prof,
                                  'user_groups':user_groups},
                                ignore_index=True)


### GENERATE USER, INSTANCE, AND LICENSE LEVEL DATAFRAMES ###

def generate_user_df(full_df, pivot_col, new_total_col):
    pivoted_df = full_df[['login','license_id','instance_url']].pivot_table(index='login', columns=pivot_col, aggfunc='count')

    # reformat column names
    pivoted_df.columns = pivoted_df.columns.droplevel(level=0)
    pivoted_df.columns.name = None
    pivoted_df = pivoted_df.reset_index()
    
    pivoted_df.fillna(0, inplace=True)
    pivoted_df[new_total_col] = pivoted_df.sum(axis=1)

    for col in pivoted_df.columns[1:]:
        pivoted_df[col] = pivoted_df[col].astype(int)

    return pivoted_df

users_licenses_df = generate_user_df(full_df, 'license_id', 'total_licenses_for_user')
users_instances_df = generate_user_df(full_df, 'instance_url', 'total_instances_for_user')

# generate license dataframe
license_df = full_df.groupby(['license_id'])['login'].nunique().to_frame().reset_index()
license_df = license_df.rename(columns={'login':'distinct_users_LICENSE'})

# generate instance dataframe
instance_df = full_df.groupby(['instance_url','license_id'])['login'].nunique().to_frame().reset_index()
instance_df = instance_df.rename(columns={'login':'distinct_users_INSTANCE'})

# merge license and instance info
merged_df = pd.merge(instance_df, license_df, on='license_id')
cols = ['instance_url','distinct_users_INSTANCE','license_id','distinct_users_LICENSE']
merged_df = merged_df[cols]

# add license limit cols
# check if it should be < OR <= !!! #######
merged_df['user_limit_LICENSE'] = user_limit_per_license
merged_df['within_limit'] = np.where(merged_df['distinct_users_LICENSE'] <= merged_df['user_limit_LICENSE'], True, False)


### WRITE TO  OUTPUT DATASETS ###

users_by_license_ds_name = get_output_names_for_role('users_by_license')[0]
users_by_license = dataiku.Dataset(users_by_license_ds_name)
users_by_license.write_with_schema(users_licenses_df)

users_by_instance_ds_name = get_output_names_for_role('users_by_instance')[0]
users_by_instance = dataiku.Dataset(users_by_instance_ds_name)
users_by_instance.write_with_schema(users_instances_df)

overall_metrics_ds_name = get_output_names_for_role('overall_metrics')[0]
overall_metrics = dataiku.Dataset(overall_metrics_ds_name)
overall_metrics.write_with_schema(merged_df)
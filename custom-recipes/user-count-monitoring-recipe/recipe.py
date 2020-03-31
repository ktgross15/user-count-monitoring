import dataikuapi
import dataiku
from dataiku.customrecipe import *
import numpy as np
import pandas as pd
import datetime
import json

### GENERATE FULL DATAFRAME ###

api_url_dict = get_recipe_config().get("urls_keys", None)
ignore_ssl_certs = get_recipe_config().get("ignore_ssl_certs", None)

def get_param_val(user, param):
    try:
        param_val = user[param]
        if param != 'groups':
            param_val = param_val.lower()
    except:
        param_val = ''
    return param_val

# instantiate lists
now = datetime.datetime.now()
prof_limit_list = []
df_data = []

for url, api_key in api_url_dict.iteritems():
    client = dataikuapi.DSSClient(url, api_key)
    if ignore_ssl_certs:
        client._session.verify = False

    # get licensing status
    licensing_status = client.get_licensing_status()
    license_id = licensing_status['base']['licenseContent']['licenseId']
    
    # add to prof limit dict
    prof_limits = licensing_status['limits']['profileLimits']
    for user_prof, vals in prof_limits.iteritems():
        limit = prof_limits[user_prof]['licensed']['licensedLimit']
        prof_limit_list.append([license_id, user_prof, limit])

    # get user-specific info
    for user in client.list_users():
        row_dict = {}
        row_dict['display_name'] = get_param_val(user, 'displayName')
        row_dict['login'] = get_param_val(user, 'login')
        row_dict['email'] = get_param_val(user, 'email')
        row_dict['user_groups'] = get_param_val(user, 'groups')
        row_dict['user_profile'] = get_param_val(user, 'userProfile')
        row_dict['license_id'] = license_id
        row_dict['instance_url'] = url
        df_data.append(row_dict)
    
    full_df = pd.DataFrame(df_data, columns=['license_id','instance_url','display_name','login','email','user_profile','user_groups'])
    full_df['time_recorded'] = now
    print "Added ", url        

# generate instance-license dataframe
instance_df = full_df.groupby(['login','user_profile','license_id','instance_url','time_recorded']).count().reset_index()
instance_df = instance_df.drop(['display_name','email','user_groups'], axis=1)

# generate overall dataframe
counts_df = full_df[['login','license_id','user_profile']].drop_duplicates().groupby(['license_id','user_profile']).count().reset_index()
counts_df = counts_df.rename(columns={'login':'count'})

limits_df = pd.DataFrame(prof_limit_list).drop_duplicates()
limits_df = limits_df.rename(columns={0:'license_id', 1:'user_profile', 2: 'limit'})
limits_df['user_profile'] = limits_df['user_profile'].str.lower()

overall_df = pd.merge(counts_df, limits_df, on=['license_id','user_profile'], how='right')
overall_df = overall_df.fillna(0)
overall_df['count'] = overall_df['count'].astype(int)
overall_df = overall_df.sort_values(by=['license_id','user_profile'])
overall_df['within_limit'] = np.where((overall_df['count']<= overall_df['limit']) | (overall_df['limit'] == -1), True, False)

### WRITE TO  OUTPUT DATASETS ###

users_by_license_ds_name = get_output_names_for_role('user_licenses')[0]
users_by_license = dataiku.Dataset(users_by_license_ds_name)
users_by_license.write_with_schema(instance_df)

overall_metrics_ds_name = get_output_names_for_role('overall_metrics')[0]
overall_metrics = dataiku.Dataset(overall_metrics_ds_name)
overall_metrics.write_with_schema(overall_df)
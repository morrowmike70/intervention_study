# -*- coding: utf-8 -*-
"""
Created on Mon Nov 28 16:19:42 2022

@author: Michael.Morrow
"""

import pandas as pd
import numpy as np
import psycopg2           # needed to connect to Redshift
import sqlalchemy as sa
import os

file_path = 'C:/Users/michael.morrow/OneDrive - Accolade, Inc/My Documents/SPI/Oct22Study/v30/'

# Set up connection to EDW database
username = os.environ.get('EDW_user')
password = os.environ.get('EDW_password')
path = 'redshift-prod.accint.io'
port = '5439'
db = 'acp_edw'

# create redshift connection string
connstr = ('postgresql+psycopg2://' + username + ':' + password +
           '@' + path + ':' + port + '/' + db)
EDW_engine = sa.create_engine(connstr)

del password, username, connstr

sql_statement = """
select distinct person_id,
       cast(utc_period as varchar(10)),
       'b2c' as intervention
from   info_layer.v1_uat_vw_care_pln_action
where  position('barrier' in action_desc)>0 and
       utc_period between '202101' and '202212';
"""         
barriers_df = pd.read_sql(sql_statement, EDW_engine)
barriers_df['utc_period'] = barriers_df['utc_period'].astype(np.int64)

intervention_df = pd.read_csv(file_path+'intervention_data_v30.csv')

intervention_df = pd.merge(intervention_df,
                           barriers_df,
                           how = 'left',
                           on = ['person_id','utc_period'])
intervention_df = intervention_df.fillna('')

intervention_df['new_category'] = intervention_df['category']+intervention_df['intervention']
intervention_df = intervention_df[['person_id',
                                   'drvd_mbrshp_covrg_id', 
                                   'utc_period',
                                   'category',
                                   'new_category']]

intervention_df.to_csv(file_path+'intervention_data_v30_b2c.csv')

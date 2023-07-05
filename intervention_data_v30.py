# -*- coding: utf-8 -*-
"""
Created on Mon Nov 28 16:19:42 2022

@author: Michael.Morrow
"""

import pandas as pd
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
select distinct pmc.person_id,
       mstr.drvd_mbrshp_covrg_id, 
       cast(enc.utc_period as varchar(10)), 
       'Benefits Guidance-FLCT' as Category
from   info_layer.v2_uat_mstr_comm mstr
       inner join info_layer.prs_mbrshp_covrg pmc on upper(mstr.drvd_mbrshp_covrg_id) = upper(pmc.drvd_mbrshp_covrg_id)
       inner join info_layer.task_dtl enc on enc.enctr_id = mstr.enctr_id
where  engmnt_flg is true
       and left(enc.est_period,4) > '2018'
       and objtv_type_nm in ('Coverage or Eligibility Confirmation', 'Benefits Question')
UNION

select distinct pmc.person_id,
       mstr.drvd_mbrshp_covrg_id, 
       cast(enc.utc_period as varchar(10)), 
       'Case Management' as Category
from   info_layer.v2_uat_mstr_comm mstr
       inner join info_layer.prs_mbrshp_covrg pmc on upper(mstr.drvd_mbrshp_covrg_id) = upper(pmc.drvd_mbrshp_covrg_id)
       inner join info_layer.task_dtl enc on enc.enctr_id = mstr.enctr_id
where  engmnt_flg is true
       and left(enc.est_period,4) > '2018'
       and objtv_type_nm in ('Program: Case Management')
UNION

select distinct pmc.person_id,
       mstr.drvd_mbrshp_covrg_id, 
       cast(enc.utc_period as varchar(10)), 
       'Transition Care' as Category
from   info_layer.v2_uat_mstr_comm mstr
       inner join info_layer.prs_mbrshp_covrg pmc on upper(mstr.drvd_mbrshp_covrg_id) = upper(pmc.drvd_mbrshp_covrg_id)
       inner join info_layer.task_dtl enc on enc.enctr_id = mstr.enctr_id
where  engmnt_flg is true
       and left(enc.est_period,4) > '2018'
       and objtv_type_nm in ('Transition Care: High Risk Program')
UNION

select distinct pmc.person_id,
       mstr.drvd_mbrshp_covrg_id, 
       cast(enc.utc_period as varchar(10)), 
       'Rising Risk' as Category
from   info_layer.v2_uat_mstr_comm mstr
       inner join info_layer.prs_mbrshp_covrg pmc on upper(mstr.drvd_mbrshp_covrg_id) = upper(pmc.drvd_mbrshp_covrg_id)
       inner join info_layer.task_dtl enc on enc.enctr_id = mstr.enctr_id
where  engmnt_flg is true
       and left(enc.est_period,4) > '2018'
       and objtv_type_nm in ('Rising Risk Care Program')
UNION

select distinct pmc.person_id,
       mstr.drvd_mbrshp_covrg_id, 
       cast(enc.utc_period as varchar(10)), 
       'Maternity Care' as Category
from   info_layer.v2_uat_mstr_comm mstr
       inner join info_layer.prs_mbrshp_covrg pmc on upper(mstr.drvd_mbrshp_covrg_id) = upper(pmc.drvd_mbrshp_covrg_id)
       inner join info_layer.task_dtl enc on enc.enctr_id = mstr.enctr_id
where  engmnt_flg is true
       and left(enc.est_period,4) > '2018'
       and objtv_type_nm in ('Program: Maternity Management')
UNION

select distinct pmc.person_id,
       mstr.drvd_mbrshp_covrg_id, 
       cast(enc.utc_period as varchar(10)), 
       'Navigation-FLCT' as Category
from   info_layer.v2_uat_mstr_comm mstr
       inner join info_layer.prs_mbrshp_covrg pmc on upper(mstr.drvd_mbrshp_covrg_id) = upper(pmc.drvd_mbrshp_covrg_id)
       inner join info_layer.task_dtl enc on enc.enctr_id = mstr.enctr_id
where  engmnt_flg is true
       and left(enc.est_period,4) > '2018'
       and objtv_type_nm in ('Find a Provider', 'Logistics', 'Appointment Scheduling', 'Check Network Status')
UNION

select distinct pmc.person_id,
       mstr.drvd_mbrshp_covrg_id, 
       cast(enc.utc_period as varchar(10)), 
       ('TDS-' || objtv_type_nm) as Category
from   info_layer.v2_uat_mstr_comm mstr
       inner join info_layer.prs_mbrshp_covrg pmc on upper(mstr.drvd_mbrshp_covrg_id) = upper(pmc.drvd_mbrshp_covrg_id)
       inner join info_layer.task_dtl enc on enc.enctr_id = mstr.enctr_id
where  engmnt_flg is true
       and left(enc.est_period,4) > '2018'
       and objtv_type_nm in ('Care Outreach','Symptoms Care','Conditions Care','Pharmacy Care Outreach',
                             'Case Management Care Outreach','Mental Health Integrated Care',
                             'Other Care Education','Preventive Care','Care Consult',
                             'Pharmacist Review','Wellness Care','Program: Enhanced RX')
UNION

select distinct enc.person_id,
       enc.drvd_mbrshp_covrg_id, 
       cast(enc.utc_period as varchar(10)),   
       'Navigation-SelfServe' as Category
from   info_layer.encounter_dtl enc
       inner join info_layer.prs_mbrshp_covrg pmc on enc.person_id = pmc.person_id
where  self_svc_eng_flg is true
       and left(enc.utc_period,4) > '2018'
       and actvy_type in ('provider-search-expand','provider-search-insights-viewed','provider-search-open')
UNION

select distinct enc.person_id,
       enc.drvd_mbrshp_covrg_id, 
       cast(enc.utc_period as varchar(10)),  
       'Benefits Guidance-SelfServe' as Category
from   info_layer.encounter_dtl enc
       inner join info_layer.prs_mbrshp_covrg pmc on enc.person_id = pmc.person_id
where  self_svc_eng_flg is true
       and left(enc.utc_period,4) > '2018'
       and actvy_type in ('benefit-program-expand', 'benefit-program-search', 'benefit-program-view-list')

"""         
intervention_df = pd.read_sql(sql_statement, EDW_engine)

intervention_df.to_csv(file_path+'intervention_data_v30.csv')

# -*- coding: utf-8 -*-
"""
December 9, 2022
In this experiment, adding some matching variables to control for geography and 
carrier. Also, each customer should be matched to another customer.

@author: Michael.Morrow
"""

import pandas as pd
import psycopg2           # needed to connect to Redshift
import sqlalchemy as sa
import os
import gc

file_path = 'C:/Users/michael.morrow/OneDrive - Accolade, Inc/My Documents/SPI/Oct22Study/v30/'
ver = 'v30'
years = [2022]

client_list = [ ('FACEBOOK','fcb','1=1','1=1'),
                  ('GEORGIA','usg',"1=1","1=1"),
                  ('BANK OF NEW YORK MELLON','bny','1=1','1=1'),
                  ('DUPONT','dup','1=1','1=1'),
                  ('FIDELITY','fid','1=1','1=1'),
                  ('SINAI','msi','1=1','1=1'),
                  ('JOHNSON CONTROLS','jci','1=1','1=1'),
                  ('AAA Club Alliance','amm','1=1','1=1'),
                  ('COMCAST','cst','1=1','1=1'),
                  ('LOWE','lws','1=1','1=1'),
                 ('STATE FARM','sfm','1=1','1=1'),
                  ('UNITED AIRLINES','una','1=1','1=1'),
                  ('AMERICAN','amr',"ins_emp_group_name in ('American Airlines','Blank')","ins_emp_group_name in ('American Airlines','Blank')"),
                  ('AMERIGAS','amg',"udf17_medical in ('Active','Flex')","udf17_pharmacy in ('Active','Flex')"),
                  ('CAREY','cri',"1=1","1=1"),
                  ('CATAPULT','ctl',"1=1","1=1"),
                  ('SEATTLE','sea',"1=1","1=1"),
                  ('EMD','emd',"udf17_medical in ('Active')","udf17_pharmacy in ('Active')","udf17 in ('Active')"),
                  ('FIRST SOLAR','fsl',"1=1","1=1"),
                  ('HARRIS REBAR','hrb',"1=1","1=1"),
                  ('INTUIT','itt',"udf17_medical in ('Active')","udf17_pharmacy in ('Active')" ),
                  ('OCEAN STATE','osj',"1=1","1=1"),
                 ###('SAUDER','sdw',"1=1"),
                 ###('SEDGWICK','sdg',"udf17_medical in ('Active')","udf17_pharmacy in ('Active')"),
                 ('TEMPLE','ths',"1=1","1=1"),                
                 ('COMMSCOPE','chc',"1=1","1=1"),
                 ('FIRST AMERICAN','faf',"1=1","1=1"),
                 ('DEVRY','dvu',"1=1","1=1"),
                 ('CSL BEHRING','csl',"1=1","1=1")
                ]

# function to add/subtract month to date format
def month_shift (month_text,add):
    text_len = len(month_text)
    if text_len<=7:
        yr_num = int(month_text[:4])
        mo_num = int(month_text[(text_len-2):]) + add
        if mo_num>12:
            mo_num = mo_num - 12
            yr_num = yr_num + 1
        elif mo_num<1:
            mo_num = mo_num + 12
            yr_num = yr_num - 1
        if text_len==7:
            new_date = str(yr_num).zfill(4) + '-' + str(mo_num).zfill(2)
        else:
            new_date = str(yr_num).zfill(4) + str(mo_num).zfill(2)
    elif month_text[2]=='-':
        mo_num = int(month_text[0:2])+add
        yr_num = int(month_text[6:])
        if mo_num>12:
            mo_num = mo_num - 12
            yr_num = yr_num + 1
        elif mo_num<1:
            mo_num = mo_num + 12
            yr_num = yr_num - 1
        new_date = str(mo_num).zfill(2) + '-01-' + str(yr_num).zfill(4)
    else:
        mo_num = int(month_text[5:7])+add
        yr_num = int(month_text[:4])
        if mo_num>12:
            mo_num = mo_num - 12
            yr_num = yr_num + 1
        elif mo_num<1:
            mo_num = mo_num + 12
            yr_num = yr_num - 1
        new_date = str(yr_num).zfill(4) +'-' + str(mo_num).zfill(2) + '-01'
    return new_date

# Redshift credentials and path
username = os.environ.get('DW_user')
password = os.environ.get('DW_password')
path = 'redshift-accolade.c5gnv3svzt6p.us-east-1.redshift.amazonaws.com'
port = '5439'
db = 'dev'

# Create redshift connection string
connstr = ('postgresql+psycopg2://' + username + ':' + password +
           '@' + path + ':' + port + '/' + db)
DW_engine = sa.create_engine(connstr)

# Set up connection to EDW database
username = os.environ.get('AEDW_user')
password = os.environ.get('AEDW_password')
path = 'redshift-prod.accint.io'
port = '5439'
db = 'aedw'

# create redshift connection string
connstr = ('postgresql+psycopg2://' + username + ':' + password +
           '@' + path + ':' + port + '/' + db)
EDW_engine = sa.create_engine(connstr)

# del password, username, connstr

# pull in intervention data from saved file
big_intervention_df = pd.read_csv(file_path+'intervention_data_' + 'v30_b2c' + '.csv')
if 'Unnamed: 0' in big_intervention_df.columns:
    big_intervention_df = big_intervention_df.drop('Unnamed: 0', axis =1)
big_intervention_df['utc_period'] = big_intervention_df['utc_period'].astype(str)
big_intervention_df.drop('category',axis=1,inplace=True)
big_intervention_df.columns=['person_id', 'drvd_mbrshp_covrg_id', 'utc_period', 'category']



for year in years:

    ver = ver + "_" + str(year)

    for c in client_list:
        client_db = c[1] 
        client_name = c[0]
        med_constraint = c[2]
        rx_constraint = c[3]
        
        # accumulator dataframes
        claims_accum_df = pd.DataFrame()
        chronic_accum_df = pd.DataFrame()
        util_accum_df = pd.DataFrame()
        elig_accum_df = pd.DataFrame()
        xwalk_accum_df = pd.DataFrame()
        SPI_accum_df = pd.DataFrame()
        
        # pull static member information and claims for the each month
        claims_begin = '10-01-'+str(year-1)
        if year == 2022:
            claims_end = '02-28-2023'
        else:
            claims_end = '05-31-'+str(year+1)
            
        name_constraint = '1=1'
        if client_db == 'amm':
            name_constraint = "ins_emp_group_name = '"+client_name+"'"
            
        sql_statement = f"""
                SELECT   'medical' as claims_type,
                         dw_member_id,
                         udf26_medical as person_id,
                         mbr_gender,
                         datediff(year, mbr_dob, '10-01-2021') as mbr_age,
    	                 mbr_relationship_code,
                         mbr_zip,
                         ins_carrier_name,
    	                 to_char(svc_service_frm_date, 'YYYY-MM') as service_month,
                         0 as scripts,
                         sum(rev_allowed_amt) as allowed
               FROM      stage1_acl_{client_db}_extract.medical	
               WHERE     svc_service_frm_date BETWEEN '{claims_begin}' AND '{claims_end}'	
                         and {med_constraint}	
                         and {name_constraint}
     	       GROUP BY  claims_type, dw_member_id, person_id, mbr_gender, mbr_age, 
    	                 mbr_relationship_code, mbr_zip, ins_carrier_name, service_month
               UNION
               SELECT    'pharmacy' as claims_type,
                         dw_member_id,
                         udf26_pharmacy as person_id,
                         mbr_gender,                     
                         datediff(year, mbr_dob, '10-01-2021') as mbr_age,
    	                 mbr_relationship_code,
                         mbr_zip,
                         ins_carrier_name,
    	                 to_char(svc_service_frm_date, 'YYYY-MM') as service_month,
                         sum(pharmacyScriptForUm) as scripts,
                         sum(rev_allowed_amt) as allowed
               FROM      stage1_acl_{client_db}_extract.pharmacy	
               WHERE     svc_service_frm_date BETWEEN '{claims_begin}' AND '{claims_end}'		
                         and {rx_constraint}
                         and {name_constraint}
     	       GROUP BY  claims_type, dw_member_id, person_id, mbr_gender, mbr_age, 
    	                 mbr_relationship_code, mbr_zip, ins_carrier_name, service_month;
             """         
        claims_df = pd.read_sql(sql_statement, DW_engine)
        
        # pull in utilization counts for select services
        name_constraint = '1=1'
        if client_db == 'amm':
            name_constraint = "groupname = '"+client_name+"'"
        sql_statement = f"""
               SELECT   dw_member_id,
                        to_char(servicedate, 'YYYY-MM') as service_month,
                        CASE WHEN categorydescription = 'Outpatient Services' Then 'Outpatient Surgery'
                             WHEN categorydescription = 'Office Procedures' THEN 'Office Surgery'
                             WHEN categorydescription in ('Inpatient Medical','Inpatient Surgical') THEN categorydescription
                             WHEN categorydescription = 'Emergency Room' THEN 'ER_visit'
                             WHEN categorydescription = 'Physician-Specialist Visit' THEN 'Spec_visit'
                             WHEN categorydescription = 'Physician-PCP Visit' THEN 'PCP_visit'
                             WHEN categorydescription = 'Physician-Preventive' THEN 'Preventive_visit'
                             WHEN categorydescription = 'Outpatient Urgent Care' THEN 'Urgent_care_visit'
                             WHEN categorydescription = 'Physician-Telehealth' THEN 'Telehealth_visit'
                             END as description,
                        eventtype,
    	                case when eventtype = 'Neither' then count(distinct dw_member_id || servicedate || primaryprovidernpi)
    	                     when eventtype = 'Reversal' then -count(distinct dw_member_id || servicedate || primaryprovidernpi)
    	                     else 0 end as count_units
               FROM     stage1_acl_{client_db}_extract.utilization
    	       WHERE    servicedate BETWEEN '{claims_begin}' AND '{claims_end}'
    	                AND categorydescription in ('Inpatient Medical','Inpatient Surgical','Emergency Room','Physician-Specialist Visit',
                                                    'Outpatient Surgery -11','Outpatient Surgery -22','Outpatient Surgery -14',
                                                    'Physician-PCP Visit','Physician-Preventive','Physician-Telehealth','Outpatient Urgent Care'
                                                 ) 
                        AND {name_constraint}
               GROUP BY dw_member_id, service_month, description, eventtype;
             """         
             
        util_df = pd.read_sql(sql_statement, DW_engine)
        
        # pull in chronic member data
        SQL = f"""
            SELECT  DISTINCT udf26 as person_id,
                    hascad,
                    hascopd,
                    hascancer,
                    hascongestiveheartfailure,
                    hasdepression,
                    hasdiabetes,
                    hashyperlipidemia,
                    hashypertension,
                    haslowbackpain,
                    haschronicpain,
                    hasosteoarthritis
            FROM    stage1_acl_{client_db}_extract.member
            WHERE   effectivedate < '{claims_end}' and
                    terminationdate > '{claims_begin}'
            """
        chronic_df = pd.read_sql(SQL, DW_engine)    
        
        # pull in crosswalk data
        if client_name.upper() in ('HUMANA'):
            SQL = f"""
                    SELECT DISTINCT upper(new_cntid) as drvd_mbrshp_covrg_id, 
                           person_id
                    FROM   rpt.work_client_view
                    WHERE  org_nm = '{client_name.upper()}';
                    """
        else:
            SQL = f"""
                    SELECT DISTINCT upper(new_cntid) as drvd_mbrshp_covrg_id, 
                           person_id
                    FROM   rpt.work_client_view
                    WHERE  org_nm LIKE '%%{client_name.upper()}%%';
                    """
        xwalk_df = pd.read_sql(SQL, EDW_engine)
        xwalk_df['drvd_mbrshp_covrg_id'] = xwalk_df['drvd_mbrshp_covrg_id'].str.upper()
        xwalk_accum_df = pd.concat([xwalk_accum_df, xwalk_df])
        
        if year == 2022:
            months = range(1,11) # thru October, should be ok with 4 months run-out
        else:
            months = range(1,13)
    
        for m in months:
            intervention_month = str(m).zfill(2)+"-01-"+str(year)
            #do this separately for each month of interest
            # create recent claims dollar variables
            base = str(year)+'-'+str(m).zfill(2)
            claim_elements = pd.DataFrame()
            
            claim_elements['dw_member_id'] = claims_df['dw_member_id']
            claim_elements['int_month'] = intervention_month
            claim_elements['customer'] = client_name
            for i in range(-3,5):
                service_month = month_shift(base,i)
                claim_elements['medical_claims'+str(i)] = 0
                claim_elements['pharmacy_claims'+str(i)] = 0
                claim_elements['medical_claims'+str(i)] = claims_df.loc[(claims_df['dw_member_id']==claim_elements['dw_member_id']) &
                                                                 (claims_df['service_month']==service_month) &
                                                                 (claims_df['claims_type']=='medical'),
                                                                 'allowed']
                claim_elements['pharmacy_claims'+str(i)] = claims_df.loc[(claims_df['dw_member_id']==claim_elements['dw_member_id']) &
                                                                 (claims_df['service_month']==service_month) &
                                                                 (claims_df['claims_type']=='pharmacy'),
                                                                 'allowed']
            elements = claim_elements.columns.drop(['dw_member_id','int_month','customer'])
            claim_elements = pd.pivot_table(claim_elements,
                                            index=['customer','dw_member_id','int_month'],
                                            values=elements)
    
            claim_elements = claim_elements.reset_index()
            
            claims_accum_df = pd.concat([claims_accum_df,claim_elements])
            del claim_elements
            
            # pull in utilization counts for select services
            # create recent claims dollar variables
            util_elements = pd.DataFrame()
            util_elements['dw_member_id'] = util_df['dw_member_id']
            util_elements['int_month'] = intervention_month
            for i in range(-3,5):
                service_month = month_shift(base,i)
                util_elements['IP_med'+str(i)] = util_df.loc[(util_df['dw_member_id']==util_elements['dw_member_id'])&
                                                             (util_df['service_month']==service_month) &
                                                             (util_df['description']=='Inpatient Medical'),'count_units']
                util_elements['IP_surgery'+str(i)] = util_df.loc[(util_df['dw_member_id']==util_elements['dw_member_id'])&
                                                             (util_df['service_month']==service_month) &
                                                             (util_df['description']=='Inpatient Surgical'),'count_units']
                util_elements['OP_surgery'+str(i)] = util_df.loc[(util_df['dw_member_id']==util_elements['dw_member_id'])&
                                                             (util_df['service_month']==service_month) &
                                                             (util_df['description']=='Outpatient Surgery'),'count_units']
                util_elements['Office_surgery'+str(i)] = util_df.loc[(util_df['dw_member_id']==util_elements['dw_member_id'])&
                                                             (util_df['service_month']==service_month) &
                                                             (util_df['description']=='Office Surgery'),'count_units']
                util_elements['ER_visit'+str(i)] = util_df.loc[(util_df['dw_member_id']==util_elements['dw_member_id']) &
                                                             (util_df['service_month']==service_month) &
                                                             (util_df['description']=='ER_visit'),'count_units']
                util_elements['PCP_visit'+str(i)] = util_df.loc[(util_df['dw_member_id']==util_elements['dw_member_id'])&
                                                             (util_df['service_month']==service_month) &
                                                             (util_df['description']=='PCP_visit'),'count_units']
                util_elements['Spec_visit'+str(i)] = util_df.loc[(util_df['dw_member_id']==util_elements['dw_member_id']) &
                                                             (util_df['service_month']==service_month) &
                                                             (util_df['description']=='Spec_visit'),'count_units']
                util_elements['Preventive'+str(i)] = util_df.loc[(util_df['dw_member_id']==util_elements['dw_member_id'])&
                                                             (util_df['service_month']==service_month) &
                                                             (util_df['description']=='Preventive_visit'),'count_units']
                util_elements['Telehealth'+str(i)] = util_df.loc[(util_df['dw_member_id']==util_elements['dw_member_id'])&
                                                             (util_df['service_month']==service_month) &
                                                             (util_df['description']=='Telehealth_visit'),'count_units']
                util_elements['Urgent_care'+str(i)] = util_df.loc[(util_df['dw_member_id']==util_elements['dw_member_id'])&
                                                             (util_df['service_month']==service_month) &
                                                             (util_df['description']=='Urgent_care_visit'),'count_units']
        
            util_elements = pd.pivot_table(util_elements,
                                           index=['dw_member_id','int_month'],
                                           values=util_elements.columns[2:])
            util_elements = util_elements.reset_index()
            util_accum_df = pd.concat([util_accum_df,util_elements])
            del util_elements
            
            # pull in eligibility to ensure everyone in test was eligible for the
            # full 8 month period (3 prior and 4 post)
            prior_start= month_shift(intervention_month,-3)
            prior_end = month_shift(intervention_month,4)
            sql_statement = f"""
                   SELECT   dw_member_id,                        
                            '{intervention_month}' as int_month
                   FROM     (
                            SELECT dw_member_id,
                                   min(ins_med_eff_date) as start_date,
                                   max(ins_med_term_date) as end_date
                            FROM  stage1_acl_{client_db}_extract.eligibility
                            GROUP BY 1)
                   WHERE    start_date <= '{prior_start}' 
                            AND end_date >= '{prior_end}';
                   """
            elig_df = pd.read_sql(sql_statement, DW_engine)  
            elig_accum_df = pd.concat([elig_accum_df,elig_df])
            del elig_df
                    
            ## Intervention pull from big df
                              
            i_month_label = str(m).zfill(2) + '-01-'+str(year)
            intervention_month = str(year)+str(m).zfill(2)        
            prior_start = month_shift(intervention_month,-3)
            last_month = month_shift(intervention_month,4)
    
            # pull in intervention data
            SPI_df = pd.merge(xwalk_df[['person_id']],
                                       big_intervention_df,
                                       on = 'person_id',
                                       how = 'inner')
            SPI_df = SPI_df[(SPI_df['utc_period']>=prior_start) &
                            (SPI_df['utc_period']<=last_month)]
            SPI_df['int_month'] = i_month_label
            SPI_df['int_count'] = 1
            
            intervention_df = SPI_df[['int_month','person_id']].copy()
            intervention_df['category'] = 'delete'        
            
            # start with lowest priority interventions and work backwards
            nonclinical_programs = ['Navigation-SelfServe',
                                    'Benefits Guidance-SelfServe',
                                    'Navigation-FLCT',
                                    'Navigation-FLCTb2c'
                                    'Benefits Guidance-FLCT',
                                    'Benefits Guidance-FLCTb2c']
            # loop through all nonclinical programs
            clean_start = month_shift(intervention_month,-3)
            clean_end = month_shift(intervention_month,3)
            for i in nonclinical_programs:
                int_members = SPI_df[(SPI_df['utc_period']==intervention_month) &
                                     (SPI_df['category'] == i)
                                     ][['person_id']].squeeze()
                if isinstance(int_members,str):
                    # do nothing
                    int_member = int_members
                else:
                    remove = SPI_df[(((SPI_df['utc_period']>=clean_start) &
                                      (SPI_df['utc_period']<intervention_month)) |
                                     ((SPI_df['utc_period']<=clean_end) &
                                      (SPI_df['utc_period']>intervention_month))) &
                                    (SPI_df['int_count']>0)]['person_id'].squeeze()
                    int_members = int_members[~int_members.isin(remove)]
                    intervention_df.loc[intervention_df['person_id'].isin(int_members),
                                        'category'] = i
    
            # loop through clinical programs. difference here is that prior interventions
            # are ok
            clinical_programs = ['TDS-Care Outreach',
                                 'TDS-Case Management Care Outreach',
                                 'TDS-Pharmacy Care Outreach'
                                 'TDS-Other Care Education',
                                 'TDS-Care Consult',
                                 'TDS-Care Consultb2c',
                                 'TDS-Other Care Education',
                                 'TDS-Program: Enhanced RX',
                                 'TDS-Pharmacist Review',
                                 'TDS-Mental Health Integrated Care',
                                 'TDS-Preventive Care',
                                 'TDS-Preventive Careb2c',
                                 'TDS-Symptoms Care',
                                 'TDS-Symptoms Careb2c',
                                 'TDS-Conditions Care',
                                 'TDS-Conditions Careb2c',
                                 'Rising Risk',
                                 'Rising Riskb2c',
                                 'Transition Care',
                                 'Transition Careb2c',
                                 'Case Management',
                                 'Case Managementb2c']
            for i in clinical_programs:
                int_members = SPI_df[(SPI_df['utc_period']==intervention_month) &
                                     (SPI_df['category'] == i)
                                     ][['person_id']].squeeze()
                if isinstance(int_members,str):
                    # do nothing
                    int_members = int_members
                else:
                    remove = SPI_df[(((SPI_df['utc_period']<=clean_end) &
                                      (SPI_df['utc_period']>intervention_month))) &
                                    (SPI_df['int_count']>0)]['person_id'].squeeze()
                    int_members = int_members[~int_members.isin(remove)]
                    intervention_df.loc[intervention_df['person_id'].isin(int_members),
                                        'category'] = i
            intervention_df = intervention_df[intervention_df['category']!='delete']
            
            SPI_accum_df = pd.concat([SPI_accum_df,intervention_df])    
            
        # delete some variables to make space
        #del big_intervention_df
    
        # create utilization variables
        util_accum_df = util_accum_df.fillna(0)
        
        # add in gender, age, zip and carrier from claims
        claims_info = claims_df[claims_df['claims_type']=='medical'].groupby('dw_member_id').first()
        claims_info = claims_info.reset_index()
        claims_info = claims_info[['dw_member_id','person_id','mbr_gender',
                                   'mbr_age','mbr_zip','ins_carrier_name']]
        pred_df = pd.merge(claims_accum_df, claims_info,
                           how = 'left',
                           on = 'dw_member_id')
        
        #del claims_accum_df, claims_df
        gc.collect()
        
        # remove all claimants who weren't eligible the full period
        # pred_df = pd.merge(pred_df,
        #                    elig_accum_df,
        #                    how = 'inner',
        #                    on = ['dw_member_id','int_month'])
        # pred_df.drop_duplicates(inplace=True)
        
        # add chronic flag variables
        pred_df = pd.merge(pred_df,
                           chronic_df,
                           how = 'left',
                           on = 'person_id')
        
        # remove claimants under age 20 and over 70
        pred_df = pred_df[(pred_df['mbr_age']>19) & (pred_df['mbr_age']<71)]
        
        # merge with utilization metrics
        pred_df = pd.merge(pred_df, 
                           util_accum_df, 
                           how = 'left', 
                           on=['dw_member_id','int_month'])
        
        # clean up nan
        pred_df = pred_df.fillna(0)
        
        # convert gender from M/F to 0/1
        pred_df.loc[pred_df['mbr_gender']=='M','mbr_gender'] = 0
        pred_df.loc[pred_df['mbr_gender']=='F','mbr_gender'] = 1
        pred_df.loc[pred_df['mbr_gender']=='U','mbr_gender'] = 2
        pred_df['mbr_gender'] = pred_df['mbr_gender'].astype('int')
        
        # # add personID to SPI    
        # SPI_data = pd.merge(SPI_accum_df,
        #                 xwalk_accum_df,
        #                 how = 'left',
        #                 on = 'drvd_mbrshp_covrg_id')
        
        # pivot SPI data based on person_id
        # SPI_data = SPI_data[['person_id','int_month','category']]
        
        
        # merge features with SPI data
        SPI_accum_df.drop_duplicates(inplace=True)
        pred_df = pd.merge(pred_df,
                         SPI_accum_df,
                         how='left',
                         on = ['person_id','int_month'])
        pred_df = pred_df.fillna(0)
        
        pred_df.drop_duplicates(inplace=True)
        #pred_df = pred_df.drop(['dw_member_id','person_id'], axis=1)
        # write data to file for modeling
        file_name = 'claims_plus_SPI_' + ver + '_' + client_name + '.csv'
        pred_df.to_csv(file_path + file_name, index = False)
        print(client_name+' file saved')



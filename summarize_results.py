# -*- coding: utf-8 -*-
"""
Created on Fri Dec 16 09:21:16 2022

produce basic comparison results between intervention and matched cohorts

@author: Michael.Morrow
"""

import pandas as pd
import numpy as np


file_path = 'C:/Users/michael.morrow/OneDrive - Accolade, Inc/My Documents/SPI/Oct22Study/v30/'
intervention_file = 'exp_intervention_consolidatedv30_2022.csv'
control_file = 'exp_control_consolidatedv30_2022.csv'
export_file = 'v30_2022_summarized_results.csv'

intervention = pd.read_csv(file_path + intervention_file)
control = pd.read_csv(file_path + control_file)

# both the intervention cohort and the control cohort should be the same size
# if not, stop the code
try:
    control.index = intervention.index
except:
    raise SystemExit('Intervention and control cohorts are different sizes.')

# apply any filters
idx = intervention.index

# create a summary by customer, servicemonth, category, and various outlier levels
# data can then be mixed and matched for analysis purposes



# compare different outcomes and place in separate table

outcomes = ['ER_visit','IP_med',
            'IP_surgery','PCP_visit','Preventive','Spec_visit','Urgent_care']
periods = range(-3,5)

results = pd.DataFrame()
results['customer'] = intervention['customer']
results['int_month'] = intervention['int_month']
results['category'] = intervention['category']
results['mbr_age'] = intervention['mbr_age']


# combine medical and pharmacy in one category and apply outlier provision
outlier_amount = 250000
for period in periods:
    int_claims = np.minimum(outlier_amount, 
                            intervention['medical_claims'+str(period)]+
                            intervention['pharmacy_claims'+str(period)])
    ctr_claims = np.minimum(outlier_amount, 
                            control['medical_claims'+str(period)]+
                            control['pharmacy_claims'+str(period)])
    results['int_claims'+str(period)] = int_claims
    results['ctr_claims'+str(period)] = ctr_claims
    

for outcome in outcomes:
    for period in periods:
        results_label = outcome+str(period)
        results['int'+results_label] = intervention[results_label]
        results['ctr'+results_label] = control[results_label]

summary = results.groupby(['customer','int_month','category']).sum()
int_count = results[['customer','int_month','category','mbr_age']].groupby([
                    'customer','int_month','category']).count()
summary['count'] = int_count

summary.to_csv(file_path + export_file)

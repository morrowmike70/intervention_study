# -*- coding: utf-8 -*-
"""
Created on Tue Feb  7 17:05:45 2023

@author: Michael.Morrow
"""

file_path = 'C:/Users/michael.morrow/OneDrive - Accolade, Inc/My Documents/SPI/Oct22Study/v30/'

import pandas as pd

client_list = ['AAA Club Alliance',
               'AMERICAN',
               'AMERIGAS',
               'BANK OF NEW YORK MELLON',
               'CAREY',
               'CATAPULT',
               'DUPONT',
               'FACEBOOK',
               'FIDELITY',
               'FIRST SOLAR',
               'GEORGIA',
               'HARRIS REBAR',
               'INTUIT',
               'JOHNSON CONTROLS',
               'LOWE',
               'OCEAN STATE',
               'SEATTLE',
               'SINAI',
               'UNITED AIRLINES'
               ]

year = 2022
data_ver = 'v30_' + str(year)

intervention = pd.DataFrame()
control = pd.DataFrame()

for c in client_list:
    client_ctrl = pd.read_csv(file_path+'exp_control_' + c + '_'+ data_ver + '.csv',low_memory=False)
    client_int = pd.read_csv(file_path+'exp_intervention_' + c + '_' + data_ver + '.csv',low_memory=False)
    
    intervention = pd.concat([intervention,client_int])
    control = pd.concat([control,client_ctrl])
    
intervention.to_csv(file_path+'exp_intervention_consolidated'+ data_ver +'.csv')
control.to_csv(file_path+'exp_control_consolidated'+ data_ver +'.csv')
    

# -*- coding: utf-8 -*-
"""
Created on Sun Oct 23 11:38:00 2022

@author: Michael.Morrow
"""

file_path = 'C:/Users/michael.morrow/OneDrive - Accolade, Inc/My Documents/SPI/Oct22Study/v30/'

import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
import faiss

client_list = ['FACEBOOK','GEORGIA','BANK OF NEW YORK MELLON','DUPONT','FIDELITY',
                'SINAI','JOHNSON CONTROLS','AAA Club Alliance','COMCAST','LOWE',
                'UNITED AIRLINES','AMERICAN','AMERIGAS','CAREY','CATAPULT',
                'SEATTLE',
               'FIRST SOLAR','HARRIS REBAR','INTUIT','OCEAN STATE',
               'TEMPLE','COMMSCOPE','FIRST AMERICAN',
               'DEVRY','CSL BEHRING']

ver = 'v30'              
years= [2022]
for year in years:
    data_ver = ver + '_' + str(year)
    
    pred_df = pd.DataFrame()               
    for c in client_list:
        file_name = 'claims_plus_SPI_' + data_ver + '_'+ c + '.csv'
        pred_df = pd.read_csv(file_path + file_name)
        pred_df.drop_duplicates(inplace=True)
        pred_df = pred_df.fillna(0)
        
        print ('Data loaded and ready for processing.')    
        
        
        interventions = ['TDS-Care Outreach',
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
                        'TDS-Conditions Careb2c'
                        'Benefits Guidance-SelfServe',
                        'Benefits Guidance-FLCT',
                        'Benefits Guidance-FLCTb2c',
                        'Navigation-FLCT',
                        'Navigation-FLCTb2c',
                        'Rising Risk',
                        'Rising Riskb2c',
                        'Transition Care',
                        'Transition Careb2c',
                        'Case Management',
                        'Case Managementb2c'
                        ]
        
        # label rows with no intervention as control
        pred_df.loc[~pred_df.category.isin(interventions),'category'] = 'control'
        
        #### SEDGWICK ONLY ####
        # pred_df['pharmacy_claims-1'] = 0
        # pred_df['pharmacy_claims-2'] = 0
        # pred_df['pharmacy_claims-3'] = 0
        # pred_df['pharmacy_claims0'] = 0
        # pred_df['pharmacy_claims1'] = 0
        # pred_df['pharmacy_claims2'] = 0
        # pred_df['pharmacy_claims3'] = 0
        # pred_df['pharmacy_claims4'] = 0
        # pred_df['medical_claims4'] = 0
        # pred_df['ER_visit4']=0
        # pred_df['IP_med4']=0
        # pred_df['IP_surgery4']=0
        # pred_df['PCP_visit4']=0
        # pred_df['Preventive4']=0
        # pred_df['Spec_visit4']=0
        # pred_df['Urgent_care4']=0
        
        # create summary variables
        pred_df['IPS_count'] = pred_df['IP_surgery0']+pred_df['IP_surgery-1']+pred_df['IP_surgery-2']+pred_df['IP_surgery-3']
        pred_df['IPM_count'] = pred_df['IP_med0']+pred_df['IP_med-1']+pred_df['IP_med-2']+pred_df['IP_med-3']
        pred_df['ER_count'] = pred_df['ER_visit0']+pred_df['ER_visit-1']+pred_df['ER_visit-2']+pred_df['ER_visit-3']
        pred_df['PCP_count'] = pred_df['PCP_visit0']+pred_df['PCP_visit-1']+pred_df['PCP_visit-2']+pred_df['PCP_visit-3']
        pred_df['Spec_count'] = pred_df['Spec_visit0']+pred_df['Spec_visit-1']+pred_df['Spec_visit-2']+pred_df['Spec_visit-3']
        pred_df['Prev_count'] = pred_df['Preventive0']+pred_df['Preventive-1']+pred_df['Preventive-2']+pred_df['Preventive-3']
        pred_df['UC_count'] = pred_df['Urgent_care0']+pred_df['Urgent_care-1']+pred_df['Urgent_care-2']+pred_df['Urgent_care-3']
        pred_df['IP_count'] = pred_df['IPS_count']+pred_df['IPM_count']
        pred_df['Phy_count'] = pred_df['PCP_count']+pred_df['Spec_count']+pred_df['Prev_count']
        
        pred_df['med_claims'] = (pred_df['medical_claims-1']+pred_df['medical_claims-2']+ 
                                 pred_df['medical_claims-3']+pred_df['medical_claims0'])
        pred_df['rx_claims'] = (pred_df['pharmacy_claims-1']+pred_df['pharmacy_claims-2']+ 
                                 pred_df['pharmacy_claims-3']+pred_df['pharmacy_claims0'])
        
        for i in range(-3,5):
            try:
                pred_df['claims'+str(i)]=pred_df['medical_claims'+str(i)]+pred_df['pharmacy_claims'+str(i)]
            except:
                pred_df['claims'+str(i)]=0
        
        # reset index
        pred_df = pred_df.reset_index()
        
        df_matched_control = pd.DataFrame()
        df_matched_treatment = pd.DataFrame()
        
        for carrier in pred_df['ins_carrier_name'].unique():
            carrier_pred_df = pred_df[pred_df['ins_carrier_name']==carrier]
            carrier_pred_df = carrier_pred_df[carrier_pred_df['mbr_zip'] != 'Blank']
            # break out of loop if there aren't any interventions
            if ((carrier_pred_df['category'] != 'control').sum() < 10):
                break
            # scale matching parameters
            sc = StandardScaler()
            data_columns = ['claims0','claims-1','claims-2','claims-3',
                            'mbr_age','mbr_zip',
                            'IP_count',
                            'ER_count', 'Phy_count','UC_count', 
                            'int_month',
                            'hascancer',
                            'hashypertension',
                            'hashyperlipidemia',
                            'hasdiabetes',
                            'hasosteoarthritis',
                            'hasdepression',
                            'haschronicpain']
            matching_df = carrier_pred_df[data_columns]
          
            matching_df['int_month']=matching_df['int_month'].str.replace('-','')
            matching_df = matching_df.replace(to_replace=['No', 'Yes'], value=[0, 1])
            matching_df = (matching_df
                           .drop(data_columns, axis = 1)
                           .join(matching_df[data_columns].apply(pd.to_numeric,
                                                                 errors='coerce')))
            matching_df.loc[matching_df['mbr_zip'].isnull(),'mbr_zip']=0
            matching_std_df = pd.DataFrame(sc.fit_transform(matching_df))
            matching_std_df.columns = matching_df.columns
            matching_std_df.index = matching_df.index
            # had been weighting the matching criteria, but may have overweighted
            matching_std_df['claims0'] = matching_std_df['claims0'] * 5
            matching_std_df['claims-1'] = matching_std_df['claims-1'] * 3
            matching_std_df['claims-2'] = matching_std_df['claims-2'] * 2
            matching_std_df['claims-3'] = matching_std_df['claims-3'] * 1
            print('Parameters scaled')
            
            control_idx = carrier_pred_df[carrier_pred_df.category=='control'].index
            control = matching_std_df.loc[control_idx]
            
            # reduce control randomly (with large groups don't need all the control lines)
            # control = control.sample(frac = 0.5, random_state = 42, replace = False)
            intervention_idx = carrier_pred_df.index[carrier_pred_df.category!='control']
            intervention = matching_std_df.loc[intervention_idx]
            print('Control and intervention database created')
            print('Interventions: ',intervention.shape[0])
            
            # build the index
            nlist = 50
            d = control.shape[1]
            quantizer = faiss.IndexFlatL2(d)
            index = faiss.IndexIVFFlat(quantizer, d, nlist)
            assert not index.is_trained
            index.train(control)                  # add vectors to the index
            assert index.is_trained
            index.add(control)
            print(index.ntotal)
            print('Matching index built')
            
            # searching
            k = 10
            index.nprobe = 10
            distances, neighbor_indexes = index.search(intervention, k)
            print(neighbor_indexes[-5:])
            print ('nearest neighbor distances determined')
            
            tracking_df = pd.DataFrame(intervention.index)
            tracking_df.columns = ['int_index']
            tracking_df['matched'] = 0
            
            matched_control = []
            percent_complete_threshhold = [.1,.25,.5,.75,.9,1.01]
            pct_ind = 0
            rows = tracking_df.shape[0]
            r = 0
            for current_index, row in tracking_df.iterrows():  # iterate over the dataframe
                #used to track progress
                r +=1
                if (r/rows>percent_complete_threshhold[pct_ind]):
                    print('Matching is '+ str(percent_complete_threshhold[pct_ind]*100).zfill(0) +
                          '% complete')
                    pct_ind +=1
                # check distances before checking to see if index has been matched or not
                if distances[current_index,0] > 50:
                    tracking_df.loc[current_index, 'matched'] = 0 # don't match
                else:
                    for idx in neighbor_indexes[current_index, :]: 
                        if idx not in matched_control:                       # this control has not been matched yet
                            tracking_df.loc[current_index, 'matched'] = idx  # record the matching
                            matched_control.append(idx)                      # add the matched to the list
                            break
                
            print('Matching is complete...')
            print('total observations in interventions:', tracking_df.shape[0])
            print('total matched observations in control:', len(matched_control))                
            
            # control have no match
            treatment_matched = tracking_df[tracking_df['matched']!=0]
            
            # matched control observation indexes
            control_matched_idx = control.iloc[treatment_matched.matched].index
            # = control_matched_idx.astype(int)  # change to int
            
            intervention_matched_idx = treatment_matched.int_index
            intervention_matched_idx = intervention_matched_idx.astype(int)
            
            # matched control and treatment
            df_matched_control = pd.concat([df_matched_control,carrier_pred_df.loc[control_matched_idx]])
            df_matched_treatment = pd.concat([df_matched_treatment,carrier_pred_df.loc[intervention_matched_idx]])
        
        of_interest = ['customer','int_month','dw_member_id','medical_claims-1',
                    'medical_claims-2','medical_claims-3','medical_claims0',
                    'medical_claims1','medical_claims2','medical_claims3',
                    'medical_claims4',
                    'pharmacy_claims-1',
                    'pharmacy_claims-2','pharmacy_claims-3','pharmacy_claims0',
                    'pharmacy_claims1','pharmacy_claims2','pharmacy_claims3',
                    'pharmacy_claims4',
                    'mbr_gender','mbr_age','mbr_zip','ins_carrier_name',
                    'hascad','hascopd','hascancer','hascongestiveheartfailure',
                    'hasdepression','hasdiabetes','haslowbackpain','haschronicpain',
                    'ER_visit-1','ER_visit-2','ER_visit-3',
                    'ER_visit0','ER_visit1','ER_visit2','ER_visit3','ER_visit4',
                    'IP_med-1','IP_med-2','IP_med-3','IP_med0',
                    'IP_med1','IP_med2','IP_med3','IP_med4',
                    'IP_surgery-1','IP_surgery-2','IP_surgery-3',
                    'IP_surgery0','IP_surgery1','IP_surgery2',
                    'IP_surgery3','IP_surgery4','PCP_visit-1','PCP_visit-2',
                    'PCP_visit-3','PCP_visit0','PCP_visit1',
                    'PCP_visit2','PCP_visit3','PCP_visit4','Preventive-1',
                    'Preventive-2','Preventive-3','Preventive0',
                    'Preventive1','Preventive2','Preventive3','Preventive4',
                    'Spec_visit-1','Spec_visit-2','Spec_visit-3',
                    'Spec_visit0','Spec_visit1','Spec_visit2',
                    'Spec_visit3','Spec_visit4','Urgent_care-1','Urgent_care-2',
                    'Urgent_care-3','Urgent_care0','Urgent_care1',
                    'Urgent_care2','Urgent_care3','Urgent_care4',
                    #'Telehealth_visit-1','Telehealth_visit-2',
                    #'Telehealth_visit-3','Telehealth_visit0','Telehealth_visit1',
                    #'Telehealth_visit2','Telehealth_visit3','Telehealth_visit4',
                    'category']
                    # 'OP_surgery-1','OP_surgery-2','OP_surgery-3','OP_surgery0',
                    # 'OP_surgery1','OP_surgery2','OP_surgery3']
        
        df_matched_control = df_matched_control[of_interest]
        df_matched_treatment = df_matched_treatment[of_interest]
        
            
        # #intervention_pred = pd.DataFrame(y_pred).loc[df_matched_treatment.index]
        
        df_matched_control.to_csv(file_path+'exp_control_' + c + '_'+ data_ver + '.csv')
        df_matched_treatment.to_csv(file_path+'exp_intervention_' + c + '_' + data_ver + '.csv')

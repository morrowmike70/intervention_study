# intervention_study
Actuarial intervention study for SPI analysis

The intervention study was designed to estimate the financial and utilization impact of Accolade's clinical and non-clinical interventions. Through a matched cohort design, members with an Accolade intervention are matched with similar members without an intervention. The difference in costs and utilization patterns after the intervention period for each of these groups is attributed to the impact of the intervention.

## Limitations

Perfect matches are impossible for all members. The study matches on multiple criteria and sometimes a close match in one category outweighs a distant match in another. For that reason, sufficient sample sizes are required for the results to be statistically credible.

This study is subject to selection bias. Members who choose to work with Accolade may be more motivated and more capable of making better health decisions than members who do not choose to work with Accolade. It can be argued that Accolade provides the expert information and support for members who are so motivated - that this is the value of Accolade and it cannot be completely disassociated with the selection bias.

## Process

This study uses a series of Python scripts to create the matched cohort data, and an Excel spreadsheet to compare the cohorts. The first Python script ("intervention_data_v30.py") pulls interventions by member and month period from EDW. It uses logic originally written by Andrew Ridella modified to pull only interventions of interest for SPI purposes. It pulls for all members and all customers and stores the results in a text file ("intervention_data_v30.csv"). This Python script simply runs a SQL query, and this step could easily bypass Python to run in SQL.

The next script in the series is "interventions_v30.py" which combines the data created above with claims information from CedarGate and other data from EDW. This script can be modified for different time periods (modifying the 'year' variable) and customers (modifying the client_list variable). As this script was modified and developed over 30 versions, it is messy and has not been optimized for performance. The script works as follows:
	1. Pull in intervention data from above
	2. Iterate across customers in client_list
		a. Pull claims by member for this customer and the appropriate time period from CedarGate
		b. Pull utilization by member for customer and for time period from CedarGate
		c. Pull in chronic indicators by member from CedarGate (CG doesn't have indicators by time period - once a identified as a diabetic, always a diabetic)
		d. Data is needed to crosswalk the interventions pulled to CedarGate data. We're using the rpt.work_view table in EDW for this
		e. Then iterate over each month in the time period
			i. Create an array of claims for each member for the months before, during and after the intervention month
			ii. Create an array of utilization stats for each member for the months before, during and after the intervention month
			iii. Pull in data from the eligibility table in CedarGate to ensure the member was eligible for coverage for the whole period
			iv. For each member, determine if they should be tagged with an intervention or part of the control group
				1) Clinical interventions take priority over non-clinical interventions (in the code, clinical interventions simply overwrite non-clinical)
				2) For non-clinical interventions, the member can't have any other interventions in the months preceding or the months following the intervention.
				3) For clinical interventions, the member can have a non-clinical intervention in the months preceding. Let's say the member has a benefits guidance intervention at time t-1 and then enrolls in case management in t0. That's OK, as it's an expected escalation and all part of the normal process of clinical enrollment. It doesn't really suggest two completely separate events.
		f. Merge all of this information together into a dataset for each customer and write this dataset into a text file labeled "claims_plus_SPI_v30_[client name].csv"

The next script is the fun one - matching_v30.py -  it matches intervention members to control members. The matching algorithm is probably a little over engineered for this purpose - we're using Faiss which was created by Meta. A more simple algorithm would likely be sufficient, but in certain past versions the matching parameters and the scale of the populations required more "industrial strength" tools. This script pulls in the "claims_plus_SPI" data table created in the last script by customer which means we only match control members to intervention members from the same customer. Previously, we matched members to those who were part of other customers. Matching within customers minimizes differences in plans and incentives. We also match based on the carrier to avoid the impact of different carriers might have on costs and utilization.

Having done all our member matching by customer, it's time to pull them all together again by running consolidate_csv.py. This just merges a bunch of files together and doesn't require much explanation.

We have all our results ready to go, now we just need to summarize them! summarize_results.py does the basic comparison between cohorts and spits out a file that is easier to work with in Excel. Certainly all the analysis could be done in Python, but this analysis tends to be fluid and Excel is easier for this kind of work. Important to note that an outlier claims level per month is applied in this script. The final export file is "v30_2022_summarized_results.csv." 

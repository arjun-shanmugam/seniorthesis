/******************************************************************************/
* Filename:   		  estimates.do
* Project:    		  Senior Thesis
* Author:     		  Arjun Shanmugam
* Date Created:       November 2nd 2022

* This file attempts to run the instrumental variable analysis.
/******************************************************************************/
include "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/src/02_analysis/exploratory_locals.do"

// Generate property values, adjusted by unit counts. 
generate unit_adjusted_total_val = total_val / units if units != 0
generate unit_adjusted_bldg_val = bldg_val / units if units != 0
generate unit_adjusted_land_val = land_val / units if units != 0
generate unit_adusted_other_val = other_val / units if units != 0
replace unit_adjusted_total_val = total_val / num_records_combined if units == 0
replace unit_adjusted_bldg_val = bldg_val / num_records_combined if units == 0
replace unit_adjusted_land_val = land_val / num_records_combined if units == 0
replace unit_adusted_other_val = other_val / num_records_combined if units == 0

// Generate judge dummies and drop rows w/ judges who heard few cases.
/*
#delimit ;
keep if inlist(court_person,
			   "Alex Mitchell",
			   "Anne Kenney Chaplin",
			   "Diana Horan",
			   "Dina Fein",
			   "Donna Salvido",
			   "Fairlie Dalton",
			   "Gustavo del Puerto",
			   "Irene Bagdoian",
			   "Jeffrey Winik") |
		inlist(court_person,
			   "Jonathan Kane",
			   "Joseph Kelleher III",
			   "Joseph Michaud",
			   "Maria Theophilis",
			   "MaryLou Muirhead",
			   "Michael Malamut",
			   "Neil Sherring",
			   "Robert Fields",
			  "Sergio Carvajal") |
        inlist(court_person,
			   "Timothy Sullivan");
#delimit cr
*/
egen cases_heard_by_judge = count(court_person), by(court_person)
drop if cases_heard_by_judge < 50
drop cases_heard_by_judge
encode court_person, generate(court_person_encoded)

// Generate city-year categorical variable. 
encode property_address_city, generate(property_address_city_encoded)
egen city_year = group(property_address_city file_year)
egen city_year_count = count(city_year), by(city_year)
drop if city_year_count < 5
drop city_year_count

// Run OLS regression estimates. 
eststo clear
local outcomes total_val bldg_val 
local controls 
foreach outcome of varlist `outcomes' {
	eststo `outcome'_naive: regress `outcome' judgment_for_plaintiff
	eststo `outcome'_twfe: regress `outcome' judgment_for_plaintiff i.file_year i.property_address_city_encoded
	eststo `outcome'_all_controls: regress `outcome' judgment_for_plaintiff i.file_year i.property_address_city_encoded
}

// Run IV regression estimates.
regress judgment_for_plaintiff i.court_person_encoded i.city_year, robust
testparm i.court_person_encoded
foreach outcome of varlist `outcomes' {
	eststo `outcome'_iv: ivregress 2sls `outcome' (judgment_for_plaintiff=i.court_person_encoded) i.city_year, robust
}

// Produce output tables.
foreach outcome of varlist `outcomes' {
	#delimit ;
	esttab `outcome'_naive `outcome'_twfe `outcome'_all_controls `outcome'_iv using "`tables_output'/`outcome'_results_table.tex",
		`universal_esttab_options' cells(b(fmt(3)) se(par fmt(2)))
		 keep(judgment_for_plaintiff) ;
	#delimit cr
}

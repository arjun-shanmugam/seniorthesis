/******************************************************************************/
* Filename:   iv_exploratory.do
* Project:    Senior Thesis
* Author:     Arjun Shanmugam
* Date Created:       November 2nd 2022

* This file attempts to run the instrumental variable analysis.
/******************************************************************************/
clear

// Load data.
include "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/src/02_analysis/01_exploratory/exploratory_locals.do"
import delimited "`cross_section_restricted'", bindquote(strict)

// Drop observations where total_val == 0.
// drop if total_val == 0
drop if bldg_val == 0

// Generate property values, adjusted by unit counts. 
generate unit_adjusted_total_val = total_val / units if units != 0
generate unit_adjusted_bldg_val = bldg_val / units if units != 0
generate unit_adjusted_land_val = land_val / units if units != 0
generate unit_adusted_other_val = other_val / units if units != 0
replace unit_adjusted_total_val = total_val / num_records_combined if units == 0
replace unit_adjusted_bldg_val = bldg_val / num_records_combined if units == 0
replace unit_adjusted_land_val = land_val / num_records_combined if units == 0
replace unit_adusted_other_val = other_val / num_records_combined if units == 0
// drop if units == 0 & bldg_val > 1000000

// Generate judge dummies and drop rows w/ judges who heard few cases.
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
egen cases_heard_by_judge = count(court_person), by(court_person)
drop if cases_heard_by_judge < 50
drop cases_heard_by_judge
encode court_person, generate(court_person_encoded)

// Generate city-year categorical variable. 
egen city_year = group(property_address_city file_year)
egen city_year_count = count(city_year), by(city_year)
drop if city_year_count < 40
drop city_year_count

// Run regression using judge dummies. 
regress judgment_for_plaintiff i.court_person_encoded i.city_year, robust nocons
testparm i.court_person_encoded
ivregress 2sls unit_adjusted_bldg_val (judgment_for_plaintiff=i.court_person_encoded) i.city_year, robust nocons



/*
// Attempt to re-run the above, but collapse dataset so that evictions in the same building are not separate records.
local outcomes unit_adjusted_total_val unit_adjusted_bldg_val unit_adjusted_land_val unit_adusted_other_val
collapse (mean) `outcomes' residualized_leniency (sum) judgment_for_plaintiff, by(geocoded_zipcode geocoded_street_address assessment_value_decided_date)

// Run first stage.
regress judgment_for_plaintiff residualized_leniency, robust

// Run second stage.
ivregress 2sls unit_adjusted_bldg_val (judgment_for_plaintiff = residualized_leniency), robust
*/

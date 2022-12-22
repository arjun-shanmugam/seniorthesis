/******************************************************************************/
* Filename:   		  DiD.do
* Project:    		  Senior Thesis
* Author:     		  Arjun Shanmugam
* Date Created:       November 2nd 2022

* This file attempts to run the DiD analysis.
/******************************************************************************/
include "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/src/02_analysis/exploratory_locals.do"

// Drop time-invariants by which we will not calculate heterogenous effects.
#delimit ;
drop defendant_atty defendant_atty_address_apt defendant_atty_address_city
defendant_atty_address_name defendant_atty_address_state
defendant_atty_address_street defendant_atty_address_zip
plaintiff_atty_address_city plaintiff_atty_address_name
plaintiff_atty_address_state plaintiff_atty_address_street
plaintiff_atty_address_zip next_fiscal_year bldg_val land_val other_val
total_val units num_records_combined court_location court_person
court_person_type defendant disposition disposition_date disposition_found
docket_history execution hasattyd hasattyp iehap inhad initiating_action
judgment_for judgment_for_pdu judgment_total plaintiff plaintiff_atty
plaintiff_atty_address_apt property_address_city property_address_full
property_address_state property_address_street property_address_zip reason
latitude longitude loc_id fy judgment_for_defendant mediated;

local time_invariants court_division duration file_date isentityd isentityp
judgment latest_docket_date file_month file_year latest_docket_year
latest_docket_month judgment_for_plaintiff dismissed defaulted heard for_cause
foreclosure no_cause non_payment for_cause_transfer foreclosure_transfer
non_payment_transfer no_cause_transfer;
#delimit cr


// Drop observations which could not be matched to Zestimates.
*** TODO: Fine tune! Figure out how far back I need to check parallel trends.
// preserve TODO: Re-add when finished
egen num_missing_zestimates = rowmiss(zestimate_period61-zestimate_period120)
drop if num_missing_zestimates > 0
drop num_missing_zestimates

// Create long panel dataset.
reshape long zestimate_period, i(case_number) j(months_from_2012_12)
encode case_number, generate(case_number_encoded)
rename zestimate_period zestimate
label variable zestimate "Zestimate"
generate year = floor((months_from_2012_12 + 11) / 12) + 2012
generate month = mod(months_from_2012_12, 12)
replace month = 12 if month == 0
replace months_from_2012_12 = months_from_2012_12 + 1

// Calculate first treated time period for each observation. 
drop if latest_docket_date == ""
generate treatment_m_from_to_2012_12 = (latest_docket_year - 2012 - 1)*12 + latest_docket_month + 1
sort case_number months_from_2012_12
replace treatment_m_from_to_2012_12 = 0 if judgment_for_plaintiff == 0
csdid zestimate, ivar(case_number_encoded) time(months_from_2012_12) gvar(treatment_m_from_to_2012_12) 

/******************************************************************************/
* Filename:   		  estimates.do
* Project:    		  Senior Thesis
* Author:     		  Arjun Shanmugam
* Date Created:       November 2nd 2022

* This file attempts to run the instrumental variable analysis.
/******************************************************************************/
import delimited "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/03_cleaned/restricted.csv", bindquote(strict) clear
 

* IV Analysis

// Generate property values, adjusted by unit counts. 
generate ln_total_val = ln(total_val)
label variable ln_total_val "Log(Total Value)"
generate ln_bldg_val = ln(bldg_val)
label variable ln_bldg_val "Log(Building Value)"
generate unit_adjusted_total_val = total_val / units if units != 0
generate unit_adjusted_bldg_val = bldg_val / units if units != 0
generate unit_adjusted_land_val = land_val / units if units != 0
generate unit_adusted_other_val = other_val / units if units != 0
replace unit_adjusted_total_val = total_val / num_records_combined if units == 0
replace unit_adjusted_bldg_val = bldg_val / num_records_combined if units == 0
replace unit_adjusted_land_val = land_val / num_records_combined if units == 0
replace unit_adusted_other_val = other_val / num_records_combined if units == 0

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
tab court_person
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
local outcomes v110
foreach outcome of varlist `outcomes' {
	eststo `outcome'_naive: regress `outcome' judgment_for_plaintiff
	estadd local city_fe "No"
	estadd local year_fe "No"
	estadd local iv_confounders_controls "No"
	estadd local iv "No"
	eststo `outcome'_twfe: regress `outcome' judgment_for_plaintiff i.file_year i.property_address_city_encoded
	estadd local city_fe "Yes"
	estadd local year_fe "Yes"
	estadd local iv_confounders_controls "No"
	estadd local iv "No"
}

// Run IV regression estimates.
regress judgment_for_plaintiff i.court_person_encoded i.city_year, robust
testparm i.court_person_encoded
local iv_confounders hasattyd hasattyp isentityp judgment for_cause
foreach outcome of varlist `outcomes' {
	eststo `outcome'_iv: ivregress 2sls `outcome' (judgment_for_plaintiff=i.court_person_encoded) i.city_year, robust
	estadd local city_fe "No"
	estadd local year_fe "No"
	estadd local iv_confounders_controls "Yes"
	estadd local iv "Yes"
}

// Produce output tables.
foreach outcome of varlist `outcomes' {
	#delimit ;
	esttab `outcome'_naive `outcome'_twfe `outcome'_iv using "`tables_output'/`outcome'_results_table.tex",
		`universal_esttab_options' 
		cells(b(star fmt(3)) se(par fmt(2)))
		keep(judgment_for_plaintiff)
		scalars("r2 $\text{R}^2$"
				"city_fe City F.E."
				"year_fe Year F.E."	
				"iv I.V. Estimate")
		title("Estimates of the Impact of Eviction")
		collabels(none);
	#delimit cr
}

// Test confoundedness of I.V.
matrix confoundedness_test_matrix = (., ., . \ ., ., . \ ., ., . \ ., ., . \ ., ., . \ ., ., . \ ., ., . \ ., ., . \ ., ., .) 
#delimit ;
matrix rownames confoundedness_test_matrix = "Defendant has attorney"
	"Plaintiff has attorney" "Defendant is an entity" "Plaintiff is an entity"
	"Money judgement" "For cause" "Forclosure" "No cause" "Non-payment of rent";
matrix colnames confoundedness_test_matrix = 
	"Partial F Statistic" "Two-Sided P-Value" "$R^2$";
#delimit cr

local potential_confounders hasattyd hasattyp isentityd isentityp judgment for_cause foreclosure no_cause non_payment
forvalues i=1/9 {
	local control: word `i' of `potential_confounders'
	scalar curr_col = `i'
	regress `control' i.court_person_encoded i.city_year
	testparm i.court_person_encoded
	matrix confoundedness_test_matrix[curr_col, 1] = r(F)
	matrix confoundedness_test_matrix[curr_col, 2] = r(p)
	matrix confoundedness_test_matrix[curr_col, 3] = e(r2)
}

#delimit ;
esttab matrix(confoundedness_test_matrix, fmt(2)) using "`tables_output'/placebo_first_stage.tex",
	`universal_esttab_options'
	title("Placebo First Stage Regressions")
	nomtitles;
#delimit cr



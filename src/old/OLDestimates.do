/******************************************************************************/
* Filename:   		  estimates.do
* Project:    		  Senior Thesis
* Author:     		  Arjun Shanmugam
* Date Created:       November 2nd 2022

* This file attempts to run the instrumental variable analysis.
/******************************************************************************/
import delimited "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/03_cleaned/restricted.csv", bindquote(strict) clear
 
// preserve
* DiD Analysis
// Drop variables which do not vary over time.
#delimit ;
drop defendant_atty defendant_atty_address_apt defendant_atty_address_city defendant_atty_address_name defendant_atty_address_state defendant_atty_address_street defendant_atty_address_zip plaintiff_atty_address_city plaintiff_atty_address_name plaintiff_atty_address_state plaintiff_atty_address_street plaintiff_atty_address_zip next_fiscal_year bldg_val land_val other_val total_val units num_records_combined;
#delimit cr
// Drop observations which could not be matched to Zestimates.
egen num_missing_zestimates = rowmiss(v2-v120)
drop if num_missing_zestimates > 0  // These observations will only have missing zestimates.
drop num_missing_zestimates
count
// Reshape.
reshape long v, i(case_number) j(months_since_2012_12)
count
rename v zestimate
label variable zestimate "Zestimate"
replace months_since_2012_12 = months_since_2012_12 - 2
// Generate time-relative-to-treatment variable.
generate treatment_m_relative_to_2012_12 = (latest_docket_year - 2012 - 1)*12 + latest_docket_month
generate t = months_since_2012_12 - treatment_m_relative_to_2012_12
drop months_since_2012_12
drop if t < -10  // Keep observations 10 months or fewer prior to treatment.
drop if t > 24  // Drop observations more than two years after treatment.
replace t = t + 10  // Make j-variable positive.

// MATCHES PYTHON SCRIPT UP TO THIS POINT

reshape wide zestimate, i(case_number) j(t)  // Reshape to assess panel balance.
egen missing_zestimates_relative = rowmiss(zestimate0-zestimate34)
drop if missing_zestimates_relative > 0
reshape long
replace t = t - 10  // Undo adjustment of j variable.
label variable t "Month (Relative to Month of Eviction Filing)"
local outcomes zestimate
set scheme s2color, perm
graph set window fontface default
foreach outcome of varlist `outcomes' {
	local `outcome'_label: var label `outcome'
	diff `outcome', t(judgment_for_plaintiff) p(t)
	#delimit ;
	collapse (mean) mean_outcome=`outcome'
			 (semean) se_outcome=`outcome'
			 (count) num_observations=`outcome',
			 by(t judgment_for_plaintiff);
	label variable mean_outcome `"``outcome'_label'"';
	label variable num_observations "Number of Observations";
	generate y_upper = mean_outcome + 1.96*se_outcome;
	generate y_lower = mean_outcome - 1.96*se_outcome;
	generate intersection_upper = .;
	by t: replace intersection_upper = y_upper[1] if y_upper[1] > y_lower[2];
	generate intersection_lower = .;
	by t: replace intersection_lower = y_lower[2] if y_upper[1] > y_lower[2];
	drop se_outcome;
	local scatters (scatter mean_outcome t if judgment_for_plaintiff == 1, color(red) msymbol(triangle) msize(small))
				   (scatter mean_outcome t if judgment_for_plaintiff == 0, color(blue) msymbol(S) msize(small));
	local ci_shading (rarea y_upper y_lower t if judgment_for_plaintiff == 1, lwidth(none) color(red*0.3))
					 (rarea y_upper y_lower t if judgment_for_plaintiff == 0, lwidth(none) color(blue*0.3));
	local ci_shading_intersection (rarea intersection_upper intersection_lower t, lwidth(none) color(purple*0.6));
	local lines (line mean_outcome t if judgment_for_plaintiff == 1, color(red))	
				(line mean_outcome t if judgment_for_plaintiff == 0, color(blue));
	local counts (scatter num_observations t if judgment_for_plaintiff == 1, color(red) msymbol(triangle) msize(small))
				   (scatter num_observations t if judgment_for_plaintiff == 0, color(blue) msymbol(S) msize(small));
	local xline (pci 300000 0 510000 0, lcolor(black) lpattern(dash));
	graph twoway `ci_shading' `ci_shading_intersection' `lines' `scatters' `xline',
		title("Trends in Property Zestimates")
		subtitle("10 Months Before to 2 Years After Filing")
		xtitle("Month Relative to Eviction Filing")
		xlabel(-10(2)24)
		ylabel(#3)
		legend(order(6 "Treatment (Evicted) Units" 7 "Control (Non-Evicted) Units"))
		note("95% confidence intervals shaded.")
		name("DiD_plot", replace);
	graph export "`figures_output'/DiD_`outcome'_trends.png", replace;
	graph twoway `counts',
		title("Number of Observed Property Zestimates Per Month")
		subtitle("10 Months Before to 2 Years After Filing")
		xtitle("Month Relative to Eviction Filing")
		xlabel(-10(2)24)
		legend(order(1 "Treatment (Evicted) Units" 2 "Control (Non-Evicted) Units"))
		name("counts_plot", replace);
	graph export "`figures_output'/DiD_`outcome'_counts.png", replace;
	#delimit cr

	
}

/*
* IV Analysis
restore
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
local outcomes ln_total_val ln_bldg_val 
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
*/



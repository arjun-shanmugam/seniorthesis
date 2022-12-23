/******************************************************************************/
* Filename:   iv_exploratory.do
* Project:    Senior Thesis
* Author:     Arjun Shanmugam
* Date Created:       November 2nd 2022

* This file attempts to run the DiD analysis.
/******************************************************************************/
clear

// Load data.
include  "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/src/old/exploratory_locals.do"
import delimited using "`panel_restricted'", bindquote(strict)

// Generate property values, adjusted by unit counts. 
generate unit_adjusted_total_val = total_val / units if units != 0
generate unit_adjusted_bldg_val = bldg_val / units if units != 0
generate unit_adjusted_land_val = land_val / units if units != 0
generate unit_adjusted_other_val = other_val / units if units != 0
replace unit_adjusted_total_val = total_val / num_records_combined if units == 0
replace unit_adjusted_bldg_val = bldg_val / num_records_combined if units == 0
replace unit_adjusted_land_val = land_val / num_records_combined if units == 0
replace unit_adjusted_other_val = other_val / num_records_combined if units == 0

// Produce DiD plots for each outcome variable.
#delimit ;
local outcomes total_val bldg_val land_val other_val unit_adjusted_total_val
	unit_adjusted_bldg_val unit_adjusted_land_val unit_adjusted_other_val;
#delimit cr

label variable total_val "Total Property Value"
label variable bldg_val "Building Value"
label variable land_val "Land Value"
label variable other_val "Other Value"
label variable unit_adjusted_total_val "Total Property Value (Unit Adjusted)"
label variable unit_adjusted_bldg_val "Building Value (Unit Adjusted)"
label variable unit_adjusted_land_val "Land Value (Unit Adjusted)"
label variable unit_adjusted_other_val "Other Value (Unit Adjusted)"

// Generate relative time variable.
generate t = fy - (file_year + 2)
label variable t "Fiscal Year (Relative to Treated Fiscal Year)"

egen id = group(loc_id case_number)
egen observed_1_year_before_treatment = count(t) if t == -1, by(id)
replace observed_1_year_before_treatment = observed_1_year_before_treatment > 0 if observed_1_year_before_treatment != .
egen observed_at_treatment = count(t) if t == 0, by(id)
replace observed_at_treatment = observed_at_treatment > 0 if observed_at_treatment != .
egen observed_post_treatment = count(t) if t == 1, by(id)
replace observed_post_treatment = observed_post_treatment > 0 if observed_post_treatment != .
xfill observed_1_year_before_treatment, i(id)
xfill observed_at_treatment, i(id)
xfill observed_post_treatment, i(id)
egen valid = rowtotal(observed_1_year_before_treatment observed_at_treatment observed_post_treatment)
tab valid
keep if valid == 3
drop observed_1_year_before_treatment observed_at_treatment observed_post_treatment valid id
sort t
drop if t < -3 | t > 1


foreach outcome of varlist `outcomes' {
	local `outcome'_label: var label `outcome'
	diff `outcome', t(judgment_for_plaintiff) p(t)
	preserve 
	#delimit ;
	collapse (mean) mean_outcome=`outcome'
			 (semean) se_outcome=`outcome'
			 (count) num_observations=`outcome',
			 by(t judgment_for_plaintiff);
	label variable mean_outcome `"``outcome'_label'"';
	
	generate y_upper = mean_outcome + 1.96*se_outcome;
	generate y_lower = mean_outcome - 1.96*se_outcome;
	drop se_outcome;
	local scatters (scatter mean_outcome t if judgment_for_plaintiff == 1, color(red))
				   (scatter mean_outcome t if judgment_for_plaintiff == 0, color(blue) msymbol(S));
	local ci_bars (rcap y_upper y_lower t if judgment_for_plaintiff == 1, color(red))
				  (rcap y_upper y_lower t if judgment_for_plaintiff == 0, color(blue));
	local lines (line mean_outcome t if judgment_for_plaintiff == 1, color(red))	
				(line mean_outcome t if judgment_for_plaintiff == 0, color(blue));
	local counts (scatter num_observations t if judgment_for_plaintiff == 1, color(red))
				   (scatter num_observations t if judgment_for_plaintiff == 0, color(blue) msymbol(S));
	twoway `scatters' `ci_bars' `lines',
		legend(order(1 "Treatment Units" 2 "Control Units"))
		name("DiD_plot", replace);
	twoway `counts',
		legend(order(1 "Treatment Units" 2 "Control Units"))
		name("counts_plot", replace);
	graph combine DiD_plot counts_plot,
		xcommon ///
		cols(1);
// 	graph export "`figures_output'/DiD_`outcome'.png", replace;
	#delimit cr
	restore
	
}




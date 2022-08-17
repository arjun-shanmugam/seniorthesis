/**********************************************************************/
/* test_first_stage.do  */

* This file runs preliminary analysis of the first stage.
/**********************************************************************/

include /Users/arjunshanmugam/Documents/GitHub/seniorthesis/04_analysis/code/analysis_locals.do

use `cleaned_eviction_data', clear

// Keep cases which were heard, defaulted, or involuntarily dismissed. 
keep if inlist(disposition_found, "Heard", "Defaulted", "Involuntary Dismissal")

// Keep cases which a judge presided over
keep if court_person_type == "judge"

// Keep cases corresponding to judges who saw at least 5 cases
egen cases_seen_by_judge = count(court_person), by(court_person)
keep if cases_seen_by_judge >= 5

// Calculate leave-one-out average for each individual.
egen sum_defendant_victory_by_judge = total(defendant_victory), by(court_person)
generate leave_one_out_avg = (sum_defendant_victory_by_judge - defendant_victory) / (cases_seen_by_judge - 1)

// Create month and year dummies.
generate filing_date = date(file_date, "MDY")
drop file_date
generate filing_month = month(filing_date)
generate filing_year = year(filing_date)

// Residualize leave-one-out average 
encode court_division, generate(court_division_encoded)
regress leave_one_out_avg i.filing_year i.court_division_encoded, robust
predict residualized_leniency, residuals

// Perform first stage regression
eststo clear
eststo: regress defendant_victory residualized_leniency, robust
label variable residualized_leniency "Residualized Leniency"
esttab using `output_tables'/first_stage.tex, booktabs scalar(F) replace collabels("Defendant Victory") label nomtitles


// Histogram of resideuals
histogram residualized_leniency, title("Histogram: Residualized Leniency")
graph export `output_figures'/histogram_residualized_leniency.png, replace

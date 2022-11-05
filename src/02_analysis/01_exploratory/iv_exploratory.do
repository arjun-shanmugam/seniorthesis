/******************************************************************************/
* Filename:   iv.do
* Project:    Senior Thesis
* Author:     Arjun Shanmugam
* Date Created:       November 2nd 2022

* This file attempts to run the instrumental variable analysis.
/******************************************************************************/
clear

// Load data.
include "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/src/02_analysis/01_exploratory/exploratory_locals.do"
import delimited "`assessor_values_restricted'", bindquote(strict)

// Drop observations where total_val == 0.
drop if total_val == 0


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
			   "Jonathan Kane"
			   "Joseph Kelleher III",
			   "Joseph Michaud",
			   "Maria Theophilis",
			   "MaryLou Muirhead",
			   "Michael Malamut",
			   "Neil Sherring",
			   "Robert Fields",
			  "Sergio Carvajal") |
        inlist(court_person,
			   "Timothy Sullivan",
			   "Michael Doherty");
#delimit cr
egen cases_heard_by_judge = count(court_person), by(court_person)
drop if cases_heard_by_judge < 5
drop cases_heard_by_judge
tabulate(court_person), generate(court_person)

// Generate city dummies and drop rows w/ cities where few cases were filed.
egen cases_heard_from_city = count(property_address_city), by(property_address_city)
drop if cases_heard_from_city < 10
drop cases_heard_from_city
tabulate property_address_city, generate(property_address_city)
 

// Run regression using judge dummies. 
regress judgment_for_defendant court_person1-court_person14, robust nocons
testparm court_person1-court_person14
ivregress 2sls total_val (judgment_for_defendant = court_person1-court_person14) i.file_year property_address_city1-property_address_city32, robust

// Run regression using residualized leniency measure. 
// Calculate leave-one-out average for each individual.
egen sum_defendant_victory_by_judge = total(judgment_for_defendant), by(court_person)
egen cases_seen_by_judge = count(court_person), by(court_person)
generate leave_one_out_avg = (sum_defendant_victory_by_judge - judgment_for_defendant) / (cases_seen_by_judge - 1)

// Calculate residuals.
regress leave_one_out_avg i.file_year property_address_city1-property_address_city32, robust
predict residualized_leniency, residuals

// Run first stage.
regress judgment_for_defendant residualized_leniency, robust

// Run second stage.
ivregress 2sls total_val (judgment_for_defendant = residualized_leniency), robust

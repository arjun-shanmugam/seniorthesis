/******************************************************************************/
* Filename:   summary_statistics.do
* Project:    Senior Thesis
* Author:     Arjun Shanmugam
* Date Created:       November 2nd 2022

* This file attempts to run the instrumental variable analysis.
/******************************************************************************/
clear

// Load unrestricted data.
include "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/src/02_analysis/01_exploratory/exploratory_locals.do"
import delimited "`cross_section_unrestricted'", bindquote(strict)

* Table 1: Sample Summary Statistics -- use two panels, one for the restricted sample and one for the unrestricted sample
// Generate indicator variables.
generate mediated = (disposition_found == "Mediated")
generate dismissed = (disposition_found == "Dismissed")
generate defaulted = (disposition_found == "Defaulted")
generate heard = (disposition_found == "Heard")
generate for_cause = (initiating_action == "SP Summons and Complaint - Cause")
generate foreclosure = (initiating_action == "SP Summons and Complaint - Foreclosure")
generate no_cause = (initiating_action == "SP Summons and Complaint - No Cause")
generate non_payment = (initiating_action == "SP Summons and Complaint - Non-payment of Rent")
generate for_cause_transfer = (initiating_action == "SP Transfer - Cause")
generate foreclosure_transfer = (initiating_action == "SP Transfer - Foreclosure")
generate non_payment_transfer = (initiating_action == "SP Transfer - Non-payment of Rent")
generate no_cause_transfer = (initiating_action == "SP Transfer- No Cause")

// Label variables and generate indicators when necessary.
label variable total_val "\hspace{0.25cm}Total property value"
label variable bldg_val "\hspace{0.25cm}Building value"
label variable land_val "\hspace{0.25cm}Land value"
label variable other_val "\hspace{0.25cm}Other value"
label variable units "\hspace{0.25cm}Units"
label variable hasattyd "\hspace{0.25cm}Defendant has attorney"
label variable hasattyp "\hspace{0.25cm}Plaintiff has attorney"
label variable mediated "\hspace{0.25cm}Case resolved through mediation"
label variable dismissed "\hspace{0.25cm}Case dismissed"
label variable defaulted "\hspace{0.25cm}Case defaulted"
label variable heard "\hspace{0.25cm}Case heard"
label variable isentityd "\hspace{0.25cm}Defendant is an entity"
label variable isentityp "\hspace{0.25cm}Plaintiff is an entity"
label variable judgment "\hspace{0.25cm}Money judgement"
label variable for_cause "\hspace{0.25cm}For cause"
label variable foreclosure "\hspace{0.25cm}Forclosure"
label variable no_cause "\hspace{0.25cm}No cause"
label variable non_payment "\hspace{0.25cm}Non-payment of rent"
label variable for_cause_transfer "\hspace{0.25cm}For cause; transferred from BMC or District Court"
label variable foreclosure_transfer "\hspace{0.25cm}Foreclosure; transferred from BMC or District Court"
label variable non_payment_transfer "\hspace{0.25cm}Non-payment of rent; transferred from BMC or District Court"
label variable no_cause_transfer "\hspace{0.25cm}No cause; transferred from BMC or District Court"

// Produce summary statistics table.
#delimit ;
local descriptive_statistics for_cause foreclosure no_cause non_payment 
for_cause_transfer foreclosure_transfer non_payment_transfer no_cause_transfer
defaulted dismissed heard mediated hasattyd isentityd judgment hasattyp isentityp total_val bldg_val land_val other_val
units;
eststo clear;
estpost tabstat `descriptive_statistics', c(stat) stat(mean sd n);
esttab using "`tables_output'/summary_statistics.tex",
  `universal_esttab_options' collabels("Mean" "S.D." "N")
  title("Summary Statistics") cells("mean(fmt(2)) sd(fmt(2)) count(fmt(0))")
  noobs
  refcat(for_cause "\emph{Panel A: Case Initiation}"
		 defaulted "\vspace{0.1em} \\ \emph{Panel B: Case Resolution}"
		 hasattyd "\vspace{0.1em} \\ \emph{Panel C: Defendant and Plaintiff Characteristics}"
		 total_val "\vspace{0.1em} \\ \emph{Panel D: Asessor Data From Fiscal Year Following Eviction Filing'}", nolabel);
#delimit cr
		 
// Load restricted cross section to produce balance table.
import delimited "`cross_section_restricted'", clear bindquote(strict)

// Produce balance table.
eststo clear
eststo totsample: estpost summarize `descriptive_statistics'
eststo treatment: estpost summarize descriptive_statistics if judgment_for_plaintiff==1
eststo control: estpost summarize descriptive_statistics if judgment_for_plaintiff==0
eststo groupdiff: estpost ttest descriptive_statistics, by(judgment_for_plaintiff)

esttab totsample treatment control groupdiff using "`tables_output'/balance_table.tex", replace ///
    cell( ///
        mean(pattern(1 1 1 0) fmt(4)) & b(pattern(0 0 0 1) fmt(4)) ///
        sd(pattern(1 1 1 0) fmt(4)) & se(pattern(0 0 0 1) fmt(2)) ///
    ) mtitle("Full sample" "Training" "Control" "Difference (3)-(2)")

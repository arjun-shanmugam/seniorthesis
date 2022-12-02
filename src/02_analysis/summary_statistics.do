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

// Produce summary statistics table.
#delimit ;
local descriptive_statistics defaulted dismissed heard mediated
hasattyd isentityd hasattyp isentityp total_val bldg_val land_val other_val
units;
eststo clear;
estpost tabstat `descriptive_statistics', c(stat) stat(mean sd n);
esttab using "`tables_output'/summary_statistics.tex",
  `universal_esttab_options' collabels("Mean" "S.D." "N")
  title("Summary Statistics") cells("mean(fmt(2)) sd(fmt(2)) count(fmt(0))")
  noobs
  refcat(defaulted "\emph{Panel A: Case Resolutions}"
		 hasattyd "\vspace{0.1em} \\ \emph{Panel B: Defendant and Plaintiff Characteristics}"
		 total_val "\vspace{0.1em} \\ \emph{Panel C: Asessor Data from Fiscal Year Following Eviction Filing'}", nolabel);

// Load restricted cross section to produce balance table.
import delimited "`cross_section_restricted'", clear bindquote(strict)



//
// eststo totsample: estpost summarize $COVS
// eststo treatment: estpost summarize $COVS if train==1
//     eststo control: estpost summarize $COVS if train==0
//     eststo groupdiff: estpost ttest $COVS, by(train)

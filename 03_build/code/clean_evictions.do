/**********************************************************************/
/* Cleans evictions data. */
/**********************************************************************/
cd ~/Documents/GitHub/seniorthesis/03_build/code
include build_locals.do

import delimited `input_data'/shanmugam_aug_v2.csv, clear

* Clean inconsistencies in judge names.
// Apostrophes are represented as mojibake by HTML.
generate court_person_name = subinstr(court_person, "&#039;", "", .) 
// Some identical names are formatted incorrectly
replace court_person_name = "Alex Valderrama" if ///
	court_person_name == "Alex J Valderrama"
replace court_person_name = "Caitlin Castillo" if ///
	inlist(court_person_name, "Caitlin; 01 Castillo", ///
	"Caitlin; 10 Castillo", "Caitlin; span Castillo")
replace court_person_name = "Cesar A Archilla" if court_person_name == "Cesar A"
replace court_person_name = "Claudia Abreau" if ///
	inlist(court_person_name, "Claudia; 08 Abreau", "Claudia; 11 Abreau", ///
	"Claudia; span Abreau")
replace court_person_name = "Dunbar D Livingston" if ///
	court_person_name == "Dunbar D"
replace court_person_name = "Erica Colombo" if /// 
	court_person_name == "Erica; 06 Colombo"
replace court_person_name = "Gregory Bartlett" if /// 
	court_person_name == "Gregory; span Bartlett"
replace court_person_name = "Gustavo A Del Puerto" if ///
	inlist(court_person_name, "Gustavo A", "Gustavo del Puerto")
replace court_person_name = "Kara Cunha" if ///
	court_person_name == "Kara; span Cunha"
replace court_person_name = " Robert T Santaniello" if ///
	court_person_name == "Robert T"
replace court_person_name = "Sergio Carvajal" if ///
	court_person_name == "Sergio E Carvajal"
replace court_person_name = "Shelly Sankar" if ///
	court_person_name == "Shelly Ann Sankar"
replace court_person_name = "Stephen Poitrast" if ///
	court_person_name == "Stephen; 08 Poitrast"
replace court_person_name = "Steven E Thomas" if ///
	court_person_name == "Steven E  Thomas"

* Clean inconsistencies in case outcomes.
replace judgment_for_pdu = "Defendant" if judgment_for_pdu == "defendant"
replace judgment_for_pdu = "Plaintiff" if judgment_for_pdu == "plaintiff"
replace judgment_for_pdu = "Unknown" if judgment_for_pdu == "unknown"

* Clean inconsistencies in case dispositions.
generate num_disposition_entries = ///
	length(disposition) - length(subinstr(disposition, "/", "", .))
//
//
// replace disposition_found = "Voluntary Dismissal" if ///
// 	strpos(disposition, "Voluntary Dismissal") | ///
// 	strpos(disposition, "Voluntary Dismissa")
//	
// replace disposition = "Voluntary Dismissal" if ///
// 	strpos(disposition, "Voluntary Dismissal") | ///
// 	strpos(disposition, "Voluntary Dismissa")
//	
// replace disposition_found = "Involuntary Dismissal" if ///
// 	strpos(disposition, "Involuntary Dismissal") | ///
// 	strpos(disposition, "Involuntary Dismissal")
//	
// replace disposition = "Involuntary Dismissal" if ///
// 	strpos(disposition, "Involuntary Dismissal") | ///
// 	strpos(disposition, "Involuntary Dismissal")
//
// replace disposition_found = "Stipulated Dismissal" if ///
// 	strpos(disposition, "Stipulation of Dismissal")
//	
// replace disposition = "Stipulated Dismissal" if ///
// 	strpos(disposition, "Stipulation of Dismissal")
//	
// replace disposition_found = "Stipulated Dismissal" if ///
// 	strpos(disposition, "Stipulation of dismissal")
//
// replace disposition = "Stipulated Dismissal" if ///
// 	strpos(disposition, "Stipulation of dismissal")
//
// replace disposition_found = "Heard" if ///
// 	strpos(disposition, "Satisfaction of judgment")
//	
// replace disposition = "Heard" if ///
// 	strpos(disposition, "Satisfaction of judgment")	
//	
// replace disposition_found = "Heard" if ///
// 	strpos(disposition, "Saitsfaction of judgment")
//	
// replace disposition = "Heard" if ///
// 	strpos(disposition, "Saitsfaction of judgment")	
//
//	
//
//	
// drop if court_person_name == "Del "  // TODO: Figure out what is going on here
// drop if court_person_name == "Plaintiff only via Zoom "  // TODO: Figure out what is going on here

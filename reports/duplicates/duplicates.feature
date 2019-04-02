Feature: Identify true duplicates in the Archivematica AIP store.

Background: Alma uses checksums and archival context to determine "true" duplicate files in their collection (i.e. the context of creation and use is identical).

Scenario: Generate a duplicate report
	  Given an AIP has been ingested
   	  When a duplicate checksum is found
	  Then a duplicates report is generated
	
Scenario Outline: Detect a true duplicate file
	          When a file's <properties> are equivalent
	          Then the files are true duplicates
	  
                  Examples:
                  | properties    |
		  | AIP dir_name  |  
		  | base_name	  |
		  | file_path     |
		  | date_modified |

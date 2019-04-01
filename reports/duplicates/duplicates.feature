Feature: Identify true duplicates in your Archivematica AIP store.

Background: A checksum match might indicate that duplicate objects exist in your Archivematica AIP archival storage but further analysis of the object’s context will determine whether you have identified a “true” duplicate in the archival sense (i.e. the context of creation and use is identical).

Scenario: Detect a true duplicate file
	Given an AIP has been ingested
	When the duplicates.py script is run 
	And a duplicate checksum is found
	Then the api-store-duplicates.csv file is generated
	When the AIP dir_name is equivalent
	When the base_name is equivalent
	When the file_path is equivalent
	When the date_modified is equivalent
	Then the files are true duplicates

# Access to Non-Work Destinations: How to go from Start to Finish—TIRP Process
### The tools and processes below can be adpated for other scenario evaluations, transit or other modes
### NOTE: Some tools in this folder were never actually used in the TIRP workflow, the ones that were are noted in the steps below
	1. Initialize directory folder
		a. Give the folder a brief name (this will be attached as a naming convention throughout the program), i.e. BDE, GoldRush, etc.
	2. Create origin and destination sets
		a. Origins
			i. Create origin selection in shapefile format
			ii. Place origin shapefile in a subfolder within the main folder, titled 0_origins
		b. Destinations
			i. Gather non-work destination data from sources
			ii. Join all destination types to singular shapefile
			iii. Assign unique identifier ("fid") to features
			iv. Create subset .csv files for each destination type with the set of "fid" that match the destination type, and a second column filled with 1s.
			v. Each <poi>.csv should be sitting in the main folder
			vi. Run 0_loadPOI2DB.py to load each <poi>.csv file to your DB, user may need to update DB credentials
			vii. Place destination shapefile in a subfolder within the main folder, titled 0_destinations
			viii. Create a .json file named <destination shapefile name>.json "destinationIDField: <fid>" as key:value pair
	3. Create Graph folder(s)
		a. Create as many subfolders within the main folder as there are unique graphs
		b. Label the folders "Graph_<scenario1>", "Graph_<scenario2>", etc.
		c. Generate your graph files and pipe the output to separate .txt file
	4. Run dual1_generateScenarios.py
		a. NOTE: Skim code and comments before starting this program, user may need to adjust hard coded scenario evaluation times
		b. Open terminal and run this program from the main folder
		c. Scenario folders will be automatically generated based on a series of user input
		d. Folders will be filled with origins, destinations, Graph files, config files
		e. Contents of each scenario are zipped and sent to AWS
		f. INPUTS: user input, folder_exclude.json
		g. OUTPUTS: folder_list.json, tt_matrix results returned for all scenarios from AWS
	5. Run dual2_assembleResults.py
		a. NOTE: skim code and comments before starting this program, best practice parameters can be replaced if need be.
		b. This program runs several supporting scripts to load and process the TT matrix data into the minimum travel times needed to reach X number of destinations. 
			i. 1_summarizeTTResults.py 
			ii. 2_calcDualAccessFromDB.py
			iii. 2_calcDualAccessFromDBAndPlot.py (alternate program to view minimum access times for 1, 2, 3,….100 destinations for individual blocks)--not used in Dual Access program sequence
			iv. 3_processDualAccessChange.py
	6. Run dual3_aggregate.py OR dual3b_aggregateByWorkers.py
		a. NOTE: This program is lengthy due to the specific nature of scenarios and table formats required by the TIRP project; however, all code can be reused easily if the INPUTS listed below are provided.
		b. This program creates new subfolders within each scenario folder and generates aggregated statistics by either jurisdiction (cities, counties, etc.) or worker demographics (income, race, education, etc.) and repeats for each POI type in the analysis (1 folder of results per POI per Scenario)
		c. This program requires the use of supporting program:
			i. 0_processResultsSubset.py
		d. INPUT: folder_list, comparisons.csv, group_list.json (associated .csv files), subset_list.json (associated .csv files)
		e. OUTPUT: Metro level, and transit serive area statistics for various corridors in .csv format and ready for import to Excel or LaTeX (e.g. B Line, D Line, E Line can each have separate workers weighted access measures calculated for them)
	7. Run dual4_produceMaps.py
		a. NOTE: Thoroughly code and comments before starting this program, many items are hard coded for the sake of the TIRP project. User is suggested to copy program and customize for individual project.
		b. This program requires the use of supporting program:
			i. autoTilemill.py
		c. INPUT: scenario_config.json
		d. OUTPUT: Complete maps with unique headings and legends

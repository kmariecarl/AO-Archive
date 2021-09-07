# Steps for calculating Park-and-Ride Accessibility
The steps and tools referenced below were developed for Task 5 of the Bus-Highway research project in 2017 and 2018.

	1. Calculate origin set to PNR set TT matrix by auto
	2. Calculate PNR set to destination set TT matrix by transit using AOBatchAnalyst0.2.2 , maxtime NOT included in output
	3. Use the split function on Mac to break apart the TT matrix file -- regardless of size. 
		a. Follow link https://www.pmg.com/blog/autosplit-large-csv-files-smaller-pieces/
			i. split -l 10000000 Transit_PNR_D_TT_69_Task6-results.csv
			ii. for i in *; do mv "$i" "$i.csv"; done
		b. Process will take approximately 45 min for a 75 gig file split on 10 million rows each, or X min for 7.5 GB.
	4. Use MatrixBreaker.py to incrementally load in the split (or not split) files and break them up by PNR.
		a. Output: origin,destination,deptime,tt
		b. Run time: 30-50 minutes
	5. Use createMinTT.py to create TT matrix files that retain the minimum TT in 15 minute windows for each OD pair.
		a. Usage: kristincarlson$pythoncreateMinTT.py -pnrlist PNRList_all.txt -or_dep Deptime_list_origin.txt -dest_dep Deptime_list_destination.txt -connect destination -dir matrixbreakerfiles/
		b. Run time: ~30 min
	6. Use percentile2Postgres.py to load the pnrMinTT files into a postgres db. Remember that the connect field is "destination."
	7. Use monetaryAccess_PNR_minTT.py to find all viable and shortest paths between the origin and destination set when forcing route through PNR stop.
		a. Output (2): Linked_wCostAccess_date, Linked_wTTAccess_date
		b.  Fields: label,deptime,TT/cost, jobs
	8. Use averageTransitLocal.py to average the results across the departure times (Note that each origin may not have a viable and shortest path through a PNR at every departure time)
		a. Output: Label,threshold,jobs
	9. Use processAccessResults4.py to compare these O + PNR + D accessibility results to some other mode with the same OD set.
Output: weighted percent, raw values, differences, raw percent at each threshold

﻿--DROP TABLE test.jobs;
--create table test.jobs (geoid10 bigint, c000 bigint);
--copy test.jobs from '/Users/kristincarlson/Dropbox/Bus-Highway/Task3/TTMatrixLink_Testing/geoid2jobs.csv' delimiter ',' csv header;
--Update test.jobs Set c000 = 0 Where c000 Is Null; 
--select c000 from test.jobs where GEOID10 in (270531052012002, 270531261003004, 270531261003049);
--commit;
--CREATE INDEX dest_x ON test.jobs (GEOID10);
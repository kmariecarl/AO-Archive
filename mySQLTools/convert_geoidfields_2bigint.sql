alter table ttmatrices.o2pnr
alter column origin type bigint using (origin::bigint);
alter table ttmatrices.o2pnr
alter column destination type int using (destination::int);

alter table ttmatrices.pnr2d15
alter column origin type bigint using (origin::bigint);
alter table ttmatrices.pnr2d15
alter column destination type bigint using (destination::bigint);

alter table ttmatrices.jobs
alter column geoid10 type bigint using (geoid10::bigint);
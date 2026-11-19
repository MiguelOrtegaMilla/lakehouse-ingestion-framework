-- A batch says where it was planned to write its data.
--
-- A batch that a run left planned is finished by the run after it, which asks the table of the
-- batch whether the data is there: if it is, the state is rolled forward, and if it is not, the
-- plan is run again (ADR-0005 §3, step 2). Until now the table that was asked was the one that
-- the configuration of the source named at that moment. A source whose table was changed
-- between the two runs would have had the other table asked, which has no batch of it: the
-- batch would be written a second time, to the new table, with its rows already in the old one.
--
-- From here on the location of the table is written down with the plan, by the run that plans
-- the batch, and the run that recovers the batch refuses to go on if its configuration names
-- another (a permanent failure).
--
-- A batch that was planned before this migration says none, and is recovered as it was until
-- now, in the table that the configuration names.
ALTER TABLE control.ingestion_batch
    ADD COLUMN target_table text  -- the location of the table, as the run that planned it had it
        CONSTRAINT target_table_says_something CHECK (target_table ~ '\S');

UPDATE control.schema_version SET version = 4;

-- No privilege is added. The engine writes the column with the INSERT of the plan, which it
-- has, and cannot change it afterwards, as it cannot change the plan: it has no UPDATE on it.

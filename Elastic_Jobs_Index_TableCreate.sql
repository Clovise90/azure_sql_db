---Pre 
--Create an elastic agent called jitelasticagent 
--Create 1 database call JOBSDB WITH PRICING TIER S1 or more in a server called jitelasticeserver(name initial)
--Create 2 dbs DB001 AND AzStudentC10DB in joinitdevserverc10(name intial)


--PART1  Creating Credentials   UNDER JOBSDB
-- Create a database master key if one does not already exist, using your own password Elastic Job Agent database  
CREATE MASTER KEY ENCRYPTION BY PASSWORD='Password1';  
  
-- Create two database scoped credentials.  
-- The credential to connect to the Azure SQL logical server, to execute jobs
CREATE DATABASE SCOPED CREDENTIAL JobRun WITH IDENTITY = 'JobUser',
    SECRET = 'Password1';

-- The credential to connect to the Azure SQL logical server, to refresh the database metadata in server
CREATE DATABASE SCOPED CREDENTIAL MasterCred WITH IDENTITY = 'MasterUser',
    SECRET = 'Password1';

--PART2	--Creating a target group and members on the servers. run in Elastic Job Agent database
--This helps you run the job on all the databases on the server
--run this on the agent database (jobsdb )
EXEC jobs.sp_add_target_group 'joinitC10ElasticGroup'

---Add target member(server)
EXEC jobs.sp_add_target_group_member
@target_group_name = 'joinitC10ElasticGroup',
@target_type = 'sqlserver',
@refresh_credential_name='MasterCred',
@server_name = 'joinitdevserverc10cl.database.windows.net'

---Add the target member(dbs)
EXEC jobs.sp_add_target_group_member
@target_group_name = 'joinitC10ElasticGroup',
@target_type = 'sqldatabase',
@server_name = 'joinitdevserverc10cl.database.windows.net',
@database_name = N'AzStudentC10DB'


-- Verify target group and members/View the recently created target group and target group members if they are created or not 
SELECT * FROM jobs.target_groups WHERE target_group_name='joinitC10ElasticGroup';
SELECT * FROM jobs.target_group_members WHERE target_group_name='joinitC10ElasticGroup'

--PART3 - Create Logins and Users on Target Server (Run in master of target server
---open a new query window for the target instance and connect to the master
---run the below and run in the master db 
--Creating logins on target master and user databases
---Create logins
CREATE LOGIN MasterUser WITH PASSWORD = 'Password1';
CREATE LOGIN JobUser WITH PASSWORD = 'Password1';

-- Users in Master
--run on master
CREATE USER MasterUser FROM LOGIN MasterUser;
CREATE USER JobUser FROM LOGIN JobUser;


--PART4
--Create Login in Agent master Database 
CREATE LOGIN MasterUser WITH PASSWORD = 'Password1';
CREATE LOGIN JobUser WITH PASSWORD = 'Password1';

--run on the agent db(jobsdb)
CREATE USER MasterUser FROM LOGIN MasterUser;
CREATE USER JobUser FROM LOGIN JobUser;

--PART5 - Create User in Target Database (AzStudentC6DB/DB001 if more db) and Grant Permissions
-- Create user
CREATE USER JobUser FROM LOGIN JobUser;
-- Grant elevated privileges
ALTER ROLE db_owner ADD MEMBER [JobUser];



--PART6 - Create the Job and Stored Procedure (Run in jobsdb)
--run on the agent db(jobsdb)
-- Create the job 1 sp_index_maintenance4
EXEC jobs.sp_add_job 
    @job_name = 'sp_index_maintenance4',
    @description = 'This Job performs index maintenance every Sunday at 12:00 AM';


--PART7 - Create the Index Maintenance Procedure (Run in jobsdb) and also in target db 
----created a stored procedure named sp_index_maintenance4 on the agent database. 
----If the index fragmentation percentage is less than 30%, then it reorganizes the index, and 
----index fragmentation is higher than 30%, than it rebuilds the entire index. Following is the code:
/*This script will be executed on agent database (elasticjobs) and the target 
- If sp_index_maintenance4 is not present in the target DB, the job fails.
*/
CREATE PROCEDURE sp_index_maintenance4
AS
BEGIN
    DECLARE 
        @SQLCmd NVARCHAR(MAX),
        @FregmentedIndexes INT,
        @i INT = 0,
        @TableName VARCHAR(500),
        @indexName VARCHAR(500),
        @SchemaName VARCHAR(500),
        @FregmentationPercent FLOAT,
        @RebuildCommand NVARCHAR(MAX);

    IF OBJECT_ID('tempdb..#FregmentedIndexes') IS NOT NULL
        DROP TABLE #FregmentedIndexes;

    CREATE TABLE #FregmentedIndexes (
        ID INT IDENTITY(1,1),
        TableName VARCHAR(500),
        indexName VARCHAR(500),
        SchemaName VARCHAR(500),
        Fregmentation_Percentage FLOAT
    );

    SET @SQLCmd = '
        SELECT DISTINCT 
            b.name, c.name, d.name, avg_fragmentation_in_percent
        FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, NULL) a
        INNER JOIN sys.tables b ON a.object_id = b.object_id
        INNER JOIN sys.indexes c ON a.object_id = c.object_id
        INNER JOIN sys.schemas d ON b.schema_id = d.schema_id
        WHERE b.schema_id > 1';

    INSERT INTO #FregmentedIndexes (TableName, indexName, SchemaName, Fregmentation_Percentage)
    EXEC sp_executesql @SQLCmd;

    SET @FregmentedIndexes = (SELECT COUNT(1) FROM #FregmentedIndexes);

    WHILE @i < @FregmentedIndexes
    BEGIN
        SELECT TOP 1 
            @TableName = TableName,
            @SchemaName = SchemaName,
            @indexName = indexName,
            @FregmentationPercent = Fregmentation_Percentage
        FROM #FregmentedIndexes;

        IF @FregmentationPercent > 30
            SET @RebuildCommand = 'ALTER INDEX [' + @indexName + '] ON [' + @SchemaName + '].[' + @TableName + '] REBUILD';
        ELSE
            SET @RebuildCommand = 'ALTER INDEX [' + @indexName + '] ON [' + @SchemaName + '].[' + @TableName + '] REORGANIZE';

        EXEC(@RebuildCommand);

        DELETE FROM #FregmentedIndexes 
        WHERE TableName = @TableName AND indexName = @indexName;

        SET @i = @i + 1;
    END
END;


---- Step 8: Add Job Step 
-- Add job step
EXEC jobs.sp_add_jobstep 
    @job_name = 'sp_index_maintenance4',
    @command = N'EXEC sp_index_maintenance4',
    @credential_name = 'JobRun',
    @target_group_name = 'joinitC10ElasticGroup';

---- Step 9: Start the Job Manually (Optional)
EXEC jobs.sp_start_job 'sp_index_maintenance4';

/*
-- Schedule the job for Sunday, November 23, 2025 at 2:00 AM MST
EXEC jobs.sp_update_job  
    @job_name = 'sp_index_maintenance4',  
    @enabled = 1,  
    @schedule_interval_type = 'Weeks',  
    @schedule_interval_count = 1,  
    @schedule_start_time = '2025-11-23T02:00:00';
*/

-- Step 10: Monitor Job Execution
SELECT 
    b.name, a.lifecycle, a.start_time, a.end_time
FROM 
    [jobs_internal].[job_executions] a
INNER JOIN 
    [jobs_internal].[jobs] b ON a.job_id = b.job_id
WHERE 
    b.name = 'sp_index_maintenance4';

    -- View all top-level execution status for all jobs
SELECT * FROM jobs.job_executions WHERE step_id IS NULL
ORDER BY start_time DESC;

---Check Fragmentation Levels
SELECT 
    dbschemas.[name] AS 'Schema',
    dbtables.[name] AS 'Table',
    dbindexes.[name] AS 'Index',
    indexstats.avg_fragmentation_in_percent,
    indexstats.page_count
FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') AS indexstats
INNER JOIN sys.tables dbtables ON dbtables.[object_id] = indexstats.[object_id]
INNER JOIN sys.schemas dbschemas ON dbtables.[schema_id] = dbschemas.[schema_id]
INNER JOIN sys.indexes dbindexes ON dbindexes.[object_id] = indexstats.[object_id]
    AND indexstats.index_id = dbindexes.index_id
WHERE indexstats.database_id = DB_ID();

 /*
 How do you know if index is optimized correctly? 
    - Fragmentation percentages are reduced.
    - Queries run faster with fewer logical reads.
    - Job execution logs show successful runs.
    - Resource usage is lower during workloads.
*/


---Create Job 2: CreateStudentRecord
-- Connect to the job database specified when creating the job agent

-- Step 1: Add a new job
EXEC jobs.sp_add_job 
    @job_name = 'CreateStudentRecord1', 
    @description = 'Create studentc10 table and insert 5 records';

-- Step 2: Add a job step to create the table
EXEC jobs.sp_add_jobstep 
    @job_name = 'CreateStudentRecord1',
    @step_name = 'CreateStudentTable',
    @command = N'
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = ''studentc10'')
        CREATE TABLE dbo.studentc10 (
            StudentId INT PRIMARY KEY,
            FirstName NVARCHAR(50),
            LastName NVARCHAR(50),
            Age INT
        );
    ',
    @target_group_name = 'joinitC10ElasticGroup',
    @credential_name = 'JobRun';   -- must match the credential you created



-- Step 3: Add a job step to insert 5 records
EXEC jobs.sp_add_jobstep 
    @job_name = 'CreateStudentRecord1',
    @step_name = 'InsertStudentRecords',
    @command = N'
        INSERT INTO dbo.studentc10 (StudentId, FirstName, LastName, Age)
        VALUES (1, ''Alice'', ''Johnson'', 20),
               (2, ''Bob'', ''Smith'', 22),
               (3, ''Carol'', ''Williams'', 19),
               (4, ''David'', ''Brown'', 21),
               (5, ''Eva'', ''Davis'', 23);
    ',
    @target_group_name = 'joinitC10ElasticGroup',
    @credential_name = 'JobRun';   -- must match the credential you created


--Step 4 start  job immediately
EXEC jobs.sp_start_job 'CreateStudentRecord1';

SELECT * FROM jobs.job_executions WHERE job_name = 'CreateStudentRecord1';
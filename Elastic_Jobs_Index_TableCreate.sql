/* ============================================================
   PREPARATION
   - Create Agent Database: JOBSDB (pricing tier S1 or higher) on server elasticserverc10.database.windows.net
   - Create Elastic Job Agent: jitelasticagent
   - Create Target Databases: DB001 and AzStudentC10DB 
     on server: joinitdevserverc10cl.database.windows.net

 How do you know if index is optimized correctly? 
    - Fragmentation percentages are reduced.
    - Queries run faster with fewer logical reads.
    - Job execution logs show successful runs.
    - Resource usage is lower during workloads.
   ============================================================ */


/* ============================================================
   PART 1: Create Credentials (RUN ON AGENT DATABASE: JOBSDB)
   ============================================================ */

-- Create a database master key (only once per agent DB)
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Password1';

-- Credential used by job steps to connect to target DBs
CREATE DATABASE SCOPED CREDENTIAL JobRun
WITH IDENTITY = 'JobUser',
     SECRET = 'Password1';

-- Credential used by agent to refresh metadata from target server
CREATE DATABASE SCOPED CREDENTIAL MasterCred
WITH IDENTITY = 'MasterUser',
     SECRET = 'Password1';


/* ============================================================
   PART 2: Create Target Group (RUN ON AGENT DATABASE: JOBSDB)
   ============================================================ */

-- Create target group
EXEC jobs.sp_add_target_group 'joinitC10ElasticGroup';

-- Add target server
EXEC jobs.sp_add_target_group_member
    @target_group_name = 'joinitC10ElasticGroup',
    @target_type = 'SqlServer',
    @refresh_credential_name = 'MasterCred',
    @server_name = 'joinitdevserverc10cl.database.windows.net';

-- Add target databases
EXEC jobs.sp_add_target_group_member
    @target_group_name = 'joinitC10ElasticGroup',
    @target_type = 'SqlDatabase',
    @server_name = 'joinitdevserverc10cl.database.windows.net',
    @database_name = N'AzStudentC10DB';

EXEC jobs.sp_add_target_group_member
    @target_group_name = 'joinitC10ElasticGroup',
    @target_type = 'SqlDatabase',
    @server_name = 'joinitdevserverc10cl.database.windows.net',
    @database_name = N'DB001';

-- Verify target group setup
SELECT * FROM jobs.target_groups WHERE target_group_name = 'joinitC10ElasticGroup';
SELECT * FROM jobs.target_group_members WHERE target_group_name = 'joinitC10ElasticGroup';


/* ============================================================
   PART 3: Create Logins & Users (RUN ON TARGET SERVER MASTER DB)
   ============================================================ */

-- Connect to master DB of joinitdevserverc10cl
CREATE LOGIN MasterUser WITH PASSWORD = 'Password1';
CREATE LOGIN JobUser WITH PASSWORD = 'Password1';

-- Create users in master DB
CREATE USER MasterUser FROM LOGIN MasterUser;
CREATE USER JobUser FROM LOGIN JobUser;


/* ============================================================
   PART 4: Create Logins & Users (RUN ON AGENT SERVER MASTER DB)
   ============================================================ */

-- Connect to master DB of jitelasticeserver
CREATE LOGIN MasterUser WITH PASSWORD = 'Password1';
CREATE LOGIN JobUser WITH PASSWORD = 'Password1';

-- Create users in JOBSDB
CREATE USER MasterUser FROM LOGIN MasterUser;
CREATE USER JobUser FROM LOGIN JobUser;


/* ============================================================
   PART 5: Create Users in Target Databases (RUN ON EACH TARGET DB)
   ============================================================ */

-- Connect to AzStudentC10DB
CREATE USER JobUser FROM LOGIN JobUser;
ALTER ROLE db_owner ADD MEMBER [JobUser];

-- Connect to DB001
CREATE USER JobUser FROM LOGIN JobUser;
ALTER ROLE db_owner ADD MEMBER [JobUser];


/* ============================================================
   PART 6: Create Job (RUN ON AGENT DATABASE: JOBSDB)
   ============================================================ */

EXEC jobs.sp_add_job
    @job_name = 'sp_index_maintenance4',
    @description = 'Performs index maintenance every Sunday at 12:00 AM';


/* ============================================================
   PART 7: Create Index Maintenance Procedure
   - MUST BE CREATED IN BOTH AGENT DB (JOBSDB) AND TARGET DBs
   ============================================================ */

-- Run this in JOBSDB, AzStudentC10DB, and DB001
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
        INNER JOIN sys.indexes c ON a.object_id = c.object_id AND a.index_id = c.index_id
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


/* ============================================================
   PART 8: Add Job Step (RUN ON AGENT DATABASE: JOBSDB)
   ============================================================ */

EXEC jobs.sp_add_jobstep
    @job_name = 'sp_index_maintenance4',
    @command = N'EXEC sp_index_maintenance4',
    @credential_name = 'JobRun',
    @target_group_name = 'joinitC10ElasticGroup';


/* ============================================================
   PART 9: Start Job Manually (Optional, RUN ON AGENT DB)
   ============================================================ */

EXEC jobs.sp_start_job 'sp_index_maintenance4';


/* ============================================================
   PART 10: Monitor Job Execution (RUN ON AGENT DB)
   ============================================================ */

SELECT b.name, a.lifecycle, a.start_time, a.end_time
FROM [jobs_internal].[job_executions] a
INNER JOIN [jobs_internal].[jobs] b ON a.job_id = b.job_id
WHERE b.name = 'sp_index_maintenance4';

SELECT * FROM jobs.job_executions WHERE step_id IS NULL
ORDER BY start_time DESC;


/* ============================================================
   JOB 2: CreateStudentRecord (RUN ON AGENT DB)
   ============================================================ */

-- Step 1: Create job
EXEC jobs.sp_add_job
    @job_name = 'CreateStudentRecord1',
    @description = 'Create studentc10 table and insert 5 records';

-- Step 2: Create table step
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
        );',
    @target_group_name = 'joinitC10ElasticGroup',
    @credential_name = 'JobRun';

-- Step 3: Insert records step
EXEC jobs.sp_add_jobstep
    @job_name = 'CreateStudentRecord1',
    @step_name = 'InsertStudentRecords',
    @command = N'
        INSERT INTO dbo.studentc10 (StudentId, FirstName, LastName, Age)
        VALUES (1, ''Alice'', ''Johnson'', 20),
               (2, ''Bob'', ''Smith'', 22),
               (3, ''Carol'', ''Williams'', 19),
               (4, ''David'', ''Brown'', 21),
               (5, ''Eva'', ''Davis'', 23);',
    @target_group_name = 'joinitC10ElasticGroup',
    @credential_name = 'JobRun';

-- Step 4: Start job immediately
EXEC jobs.sp_start_job 'CreateStudentRecord1';

-- Monitor execution
SELECT * FROM jobs.job_executions WHERE job_name = 'CreateStudentRecord1

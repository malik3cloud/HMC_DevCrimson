CREATE PROCEDURE [etl].[JobAuditStart] (
	@JobName NVARCHAR(500) = null,
	@TaskKeyList NVARCHAR(1000) = null,
	@FabricWorkspaceID NVARCHAR(200) = null,
	@PipelineRunID uniqueidentifier = null
)
AS
/*************************************************************************************************************************************************
PURPOSE:
High level structure of this stored procedure: 
1. Determine whether this is an "ad-hoc" or "defined" job. This is determined based on which parameters are populated. 
2. Check the status of the prior job
3. Perform the appropriate job action - Create New Job or Restart existing Job.

**************************************************************************************************************************************************/

DECLARE @LogLevel INT = 1 -- 1 is normal, 2 is verbose
DECLARE @LogMessage NVARCHAR(4000)
DECLARE @MaxJobTaskRunOrderNbr INT
DECLARE @ProcessingMethod NVARCHAR(50)
DECLARE @CurrentUTCDateTime DATETIME 
SET @CurrentUTCDateTime = GETUTCDATE()
SET @PipelineRunID = lower(@PipelineRunID)

DECLARE @PipelineRunID_string NVARCHAR(100)
SET @PipelineRunID_string = lower(convert(nvarchar(100),@PipelineRunID))

DECLARE @JobType NVARCHAR(50)

DECLARE @OriginalTaskKeyList NVARCHAR(1000) = ISNULL(@TaskKeyList, '') -- record this here for posterity
SET @TaskKeyList = NULLIF(@TaskKeyList, RTRIM(LTRIM(''))) -- convert blanks to null

DECLARE @OriginalJobName NVARCHAR(500) = ISNULL(TRIM(@JobName), '(NULL)') -- record this here for posterity
SET @JobName = NULLIF(@JobName, RTRIM(LTRIM(''))) -- convert both blanks to null

-- Priority Execution Order is TaskKeyList, JobName
-- If the TaskKeyList parameter is populated, then run hydrator for the comma-delimited list of tasks
IF @TaskKeyList IS NOT NULL OR @TaskKeyList != ''
	BEGIN
		SET @LogMessage = 'TaskKeyList = "' + @OriginalTaskKeyList + '" job triggered by fabric workspace "' + @FabricWorkspaceID + '"'
		SET @JobType = 'TaskKeyList'
		SET @JobName = 'TaskKeyList = "' + @OriginalTaskKeyList + '"'
	END
 -- If the JobName is populated, then runs all tasks for that job
ELSE IF @JobName IS NOT NULL
	BEGIN
		SET @LogMessage = '"' + @JobName + '" job triggered by fabric workspace "' + @FabricWorkspaceID + '"'
		SET @JobType = CASE WHEN lower(@JobName) IN (SELECT lower(JobName) FROM etl.Job) THEN 'Defined' ELSE 'Unknown' END
	END
ELSE 
	SET @JobType = 'Unknown'

EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = NULL, @PipelineRunID = @PipelineRunID, @Message = @LogMessage

--Specify if the Job is Defined or Ad-hoc
DECLARE @IsAdHocJob BIT
SET @IsAdHocJob = CASE WHEN @JobType = 'Defined' THEN 0 ELSE 1 END

SET @LogMessage = '"' + @JobName + '" is' + CASE WHEN @IsAdHocJob = 1 THEN '' ELSE ' NOT' END + ' an ad-hoc job.'
EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = NULL, @PipelineRunID = @PipelineRunID, @Message = @LogMessage

SET @LogMessage = '"' + @JobName + '" job will be evaulated as a "' + @JobType + '" job type.'
EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = NULL, @PipelineRunID = @PipelineRunID, @Message = @LogMessage

DECLARE @checkLastJobExecution BIT

-- Restartability default to 1 to always check for prior execution of the job
SET @checkLastJobExecution = 1

DECLARE @attemptJobRestart BIT = 0 -- controls whether the ETL will ultimately attempt to pick up from a prior job execution
DECLARE @restartJobAuditKey BIGINT -- if a restart needs to be attempted, this is the JobAuditKey that it will try to restart
DECLARE @lastPipelineRunID_string nvarchar(100)
IF @checkLastJobExecution = 1
BEGIN

        SET @LogMessage = '"' + @JobName + '" job is checking for an incomplete prior execution'
        EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = NULL, @PipelineRunID = @PipelineRunID, @Message = @LogMessage

        -- look in the JobAudit table for the most recent (as determined by job StartTime. if the job is marked as failed, then attempt a restart of that job
        DECLARE @StaleJobCutoff DECIMAL(20,2) = (SELECT TOP 1 TRY_CONVERT(DECIMAL(20,2), ConfigValue) from etl.GetGlobalConfig('StaleJobCutoff')) -- only attempt to restart jobs that were originally started within the last @StaleJobCutoff hours. this is to prevent the restartability feature from grabbing a very old failed job

        -- for Defined jobs, match on the JobKey, for Ad-hoc jobs, we'll have to just go off the JobName
        SELECT TOP 1
                @restartJobAuditKey = JobAuditKey
        ,       @lastPipelineRunID_string = (SELECT TOP 1 PipelineRunID FROM etl.Log WHERE JobAuditKey = JobAudit.JobAuditKey AND PipelineRunID IS NOT NULL ORDER BY LogDatetime DESC)
        ,       @attemptJobRestart = CASE WHEN JobAudit.Status = 'Failed' THEN 1 ELSE 0 END -- might want to instead defaulting to 1 unless the job has succeeded
        FROM etl.JobAudit
        LEFT OUTER JOIN etl.Job
                ON JobAudit.JobKey = Job.JobKey
        WHERE
                -- select on the appropriate job name
                (
                        (@IsAdHocJob = 1 AND JobAudit.IsAdHocJob = 1 AND JobAudit.JobName = @JobName) OR
                        (@IsAdHocJob = 0 AND JobAudit.IsAdHocJob = 0 AND Job.JobName = @JobName)
                )
                -- Stale Job Timer
                AND DATEADD(HOUR, -1 * @StaleJobCutoff, @CurrentUTCDateTime) <= JobAudit.StartTime
                --AND JobAudit.Status = 'Failed' -- do not filter on status. simply get the most recent execution of this job. if it failed, then attempt a rerun. if it succeeded, then perform a new run.
        ORDER BY JobAudit.StartTime DESC

        -- if there are jobs in a "job started" phase that are outside the restartability window, then go back to the JobAudit table and append to the status to denote that these jobs are now abandoned
	    UPDATE JobAudit
		    SET JobAudit.Status = JobAudit.Status + ' (abandoned)', LastUpdated = @CurrentUTCDateTime
	    FROM etl.JobAudit
	    LEFT OUTER JOIN etl.Job
		    ON JobAudit.JobKey = Job.JobKey
	    WHERE
		    JobAudit.Status NOT IN ('Success')
		    AND JobAudit.Status NOT LIKE '%(abandoned)%' -- if it's already marked, no need to mark it again
		    -- select on the appropriate job name
		    AND
		    (
			    (@IsAdHocJob = 1 AND JobAudit.IsAdHocJob = 1 AND JobAudit.JobName = @JobName) OR
			    (@IsAdHocJob = 0 AND JobAudit.IsAdHocJob = 0 AND Job.JobName = @JobName)
		    )
		    -- job falls outside the Stale Job window
		    AND DATEADD(HOUR, -1 * @StaleJobCutoff, @CurrentUTCDateTime) > JobAudit.StartTime

END

-- if we are attempting a restart, then update the status on the JobAudit record and return the JobAuditKey from the stored proc.
-- otherwise, this is a fresh job run, in which case we need to prepare the work list of ETL tasks for this job

IF @attemptJobRestart = 1 AND @restartJobAuditKey > 0
BEGIN
        SET @LogMessage = '"' + @JobName + '" job found a prior incomplete job execution under JobAuditKey ' + CONVERT(NVARCHAR(20), @restartJobAuditKey) + ' (PipelineRunID ' + ISNULL(@lastPipelineRunID_string, 'unknown') + '). This JobAuditKey will be re-executed under a new PipelineRunID (' + @PipelineRunID_string + ').'
        EXEC etl.LogMessage @JobAuditKey = @restartJobAuditKey, @TaskAuditKey = NULL, @PipelineRunID = @PipelineRunID, @Message = @LogMessage

        UPDATE etl.JobAudit SET RestartTime = @CurrentUTCDateTime, LastUpdated = @CurrentUTCDateTime, Status = 'Restarted' WHERE JobAuditKey = @restartJobAuditKey

        -- perhaps there is a better way to handle this. if a job is restarted, but for some reason some of the TaskAudit rows for that job did not get their IsRunning flag reset to 0, then this will cause issues.
        -- If we are re-running a failed job, then we will forcefully reset this flag for all rows now. This really shouldn't happen if the framework is working correctly, so this would be a notable thing to log
        UPDATE etl.TaskAudit SET IsRunning = 0, LastUpdated = @CurrentUTCDateTime WHERE JobAuditKey = @restartJobAuditKey AND IsRunning = 1
        DECLARE @RunningTaskCount INT = @@ROWCOUNT
        IF @RunningTaskCount > 0
        BEGIN
                SET @LogMessage = CONVERT(NVARCHAR(5), @RunningTaskCount) + ' tasks in the etl.TaskAudit table for JobAuditKey ' + CONVERT(NVARCHAR(20), @restartJobAuditKey) + ' still had the IsRunning flag set to true/1. These were reset to false/0.'
                EXEC etl.LogMessage @JobAuditKey = @restartJobAuditKey, @TaskAuditKey = NULL, @PipelineRunID = @PipelineRunID, @Message = @LogMessage
        END

		-- check whether to iterate tasks in batch, sequential, or combined
		SELECT 
			@ProcessingMethod = CASE 
									WHEN COUNT(DISTINCT TaskRunOrderNbr) = 1 THEN 'Parallel' -- check if all TaskRunOrderNbr are the same
									WHEN COUNT(TaskRunOrderNbr) = COUNT(DISTINCT TaskRunOrderNbr) THEN 'Sequential' -- check if all TaskRunOrderNbr are different
									ELSE 'SequentialAndParallel' 
								END 
		FROM etl.TaskAudit 
		WHERE JobAuditKey = @restartJobAuditKey

        -- return the existing JobAuditKey value
        SELECT @restartJobAuditKey AS JobAuditKey, @ProcessingMethod AS ProcessingMethod
				
END
ELSE
BEGIN
	-- we are not attempting a restart of a prior job. 
	-- this means we need to prepare the worklist of tasks for this job, create a row in the JobAudit table, and return the JobAuditKey from the new row

	SET @LogMessage = '"' + @JobName + '" job did not find a prior incomplete job execution. Generating a new JobAuditKey.'
	EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = NULL, @PipelineRunID = @PipelineRunID, @Message = @LogMessage

	DECLARE @newJobAuditKey BIGINT
	DECLARE @JobStatus NVARCHAR(200) = 'Job created'
	DECLARE @JobKey INT = 0

	IF @JobType = 'TaskKeyList'
		BEGIN
		WITH TaskCTE AS (
			SELECT TaskKey FROM etl.Task WHERE TaskKey IN (SELECT value FROM STRING_SPLIT(@TaskKeyList,',')) AND Task.IsActiveFlag = 1
			)
		SELECT TOP 1 @JobKey = TaskKey FROM TaskCTE
		ORDER BY TaskKey
		END
	ELSE IF @JobType = 'Defined'
		BEGIN
		SELECT TOP 1 @JobKey = JobKey FROM etl.Job WHERE JobName = @JobName AND Job.IsActiveFlag = 1 ORDER BY JobKey
		END

	-- first we need the job key. if we cannot find a job with the supplied JobName, then update the status message and throw an error
	DECLARE @ErrorMessage NVARCHAR(2048);
	IF @JobType = 'Defined' AND @JobKey = 0
	BEGIN
		SET @ErrorMessage =  'Error: job "' + @JobName + '" not active or not found in the etl.Job table.';
		EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = NULL, @PipelineRunID = @PipelineRunID, @Message = @ErrorMessage;
		THROW 50000, @ErrorMessage, 1;
	END
	ELSE IF @JobType = 'TaskKeyList' AND @JobKey = 0
	BEGIN
		SET @ErrorMessage =  'Error: job "' + @JobName + '" has no tasks or inactive tasks in etl Task tables.';
		EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = NULL, @PipelineRunID = @PipelineRunID, @Message = @ErrorMessage;
		THROW 50000, @ErrorMessage, 1;
	END
	ELSE
	BEGIN
		BEGIN TRANSACTION; -- perform the following in a transaction so that we can roll back the JobAudit row if no tasks are found

		DECLARE @JobAuditMaxID AS BIGINT

		IF EXISTS(SELECT * FROM etl.JobAudit)
			SET @JobAuditMaxID = (SELECT MAX(JobAuditKey) FROM etl.JobAudit)
		ELSE
			SET @JobAuditMaxID = 0

		INSERT INTO etl.JobAudit (JobAuditKey, JobKey, JobName, IsAdHocJob, StartTime, Status, FabricWorkspaceID, PipelineRunID, LastUpdated)
		VALUES (@JobAuditMaxID + 1, @JobKey, @JobName, @IsAdHocJob, @CurrentUTCDateTime, @JobStatus, @FabricWorkspaceID, @PipelineRunID_string, @CurrentUTCDateTime)

		DECLARE @JobAuditKey BIGINT
		SET @JobAuditKey = @JobAuditMaxID + 1

		SET @LogMessage = '"' + @JobName + '" job generated JobAuditKey ' + CONVERT(NVARCHAR(20), @JobAuditKey)
		EXEC etl.LogMessage @JobAuditKey = @JobAuditKey, @TaskAuditKey = NULL, @PipelineRunID = @PipelineRunID, @Message = @LogMessage;
	
		DECLARE @TaskAuditMaxID AS BIGINT

		IF EXISTS(SELECT * FROM etl.TaskAudit)
			SET @TaskAuditMaxID = (SELECT MAX(TaskAuditKey) FROM etl.TaskAudit)
		ELSE
			SET @TaskAuditMaxID = 0
		;

		WITH TaskCTE AS (
			SELECT 
				Task.TaskKey, 
				Task.TaskType, 
				ISNULL(Task.TaskRunOrderNbr, 0) AS TaskRunOrderNbr,
				Task.IsActiveFlag
			FROM etl.Task 
			LEFT OUTER JOIN etl.Job
			ON Task.JobKey = Job.JobKey
			WHERE ISNULL(Task.IsActiveFlag, 1) = 1
			AND ISNULL(Job.IsActiveFlag, 1) = 1
			AND (
				(@JobType = 'Defined' AND Job.JobName = @JobName AND ISNULL(Task.IsActiveFlag, 1) != 0) -- Explicit jobs: all tasks mapped to this specific job
			OR  (@JobType = 'TaskKeyList' AND Task.TaskKey IN (SELECT value FROM STRING_SPLIT(@TaskKeyList, ','))) -- TaskKey jobs: just execute the list of TaskKeys supplied
			)

			UNION ALL

			SELECT 
				Task.FileTaskKey, 
				Task.TaskType, 
				ISNULL(Task.TaskRunOrderNbr, 0) AS TaskRunOrderNbr,
				Task.IsActiveFlag
			FROM etl.FileTask Task
			LEFT OUTER JOIN etl.Job
			ON Task.JobKey = Job.JobKey
			WHERE ISNULL(Task.IsActiveFlag, 1) = 1
			AND ISNULL(Job.IsActiveFlag, 1) = 1
			AND (
				(@JobType = 'Defined' AND Job.JobName = @JobName AND ISNULL(Task.IsActiveFlag, 1) != 0) -- Explicit jobs: all tasks mapped to this specific job
			OR  (@JobType = 'TaskKeyList' AND Task.FileTaskKey IN (SELECT value FROM STRING_SPLIT(@TaskKeyList, ','))) -- TaskKey jobs: just execute the list of TaskKeys supplied
			)
		)
		INSERT INTO etl.TaskAudit (TaskAuditKey, JobAuditKey, TaskKey, TaskType, Status, IsRunning, TaskRunOrderNbr, LastUpdated,PipelineRunID)
		SELECT DISTINCT
		  @TaskAuditMaxID + ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS TaskAuditKey -- fabric warehouse workaround for identity column
		, @JobAuditKey
		, Task.TaskKey
		, Task.TaskType
		, 'Task created' AS Status
		, 0 AS IsRunning
        , Task.TaskRunOrderNbr
		,  @CurrentUTCDateTime AS LastUpdated
        , @PipelineRunID_string
		FROM TaskCTE AS Task

		-- get # of rows just inserted into TaskAudit and log IT
		DECLARE @TaskCount INT = @@ROWCOUNT

        -- check whether to iterate tasks in batch, sequential, or combined
		SELECT 
			@ProcessingMethod = CASE 
									WHEN COUNT(DISTINCT TaskRunOrderNbr) = 1 THEN 'Parallel' -- check if all TaskRunOrderNbr are the same
									WHEN COUNT(TaskRunOrderNbr) = COUNT(DISTINCT TaskRunOrderNbr) THEN 'Sequential' -- check if all TaskRunOrderNbr are different
									ELSE 'SequentialAndParallel' 
								END 
		FROM etl.TaskAudit 
		WHERE JobAuditKey = @JobAuditKey 

		SET @LogMessage = '"' + @JobName + '" job will attempt to execute ' + CONVERT(NVARCHAR(5), @TaskCount) + ' tasks.'
		EXEC etl.LogMessage @JobAuditKey = @JobAuditKey, @TaskAuditKey = NULL, @PipelineRunID = @PipelineRunID, @Message = @LogMessage
        
		IF @TaskCount > 0 
		BEGIN 
			COMMIT TRANSACTION; 

		-- return the newly-created JobAuditKey value
		SELECT @JobAuditKey AS JobAuditKey, @ProcessingMethod AS ProcessingMethod

	END
	ELSE
	BEGIN
		-- if no tasks were actually located, then there is no job to perform, and we don't want a permanent JobAudit record afterall. 
		ROLLBACK TRANSACTION; 

		SET @LogMessage = 'No tasks were actually located for this particular job. No JobAuditKey will be created.'
		EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = NULL, @PipelineRunID = @PipelineRunID, @Message = @LogMessage

		-- Data Factory still requires a return value from this stored procedure
		SELECT CONVERT(INT, 0) AS JobAuditKey, CONVERT(NVARCHAR(50), '') AS ProcessingMethod
	END
	END
END
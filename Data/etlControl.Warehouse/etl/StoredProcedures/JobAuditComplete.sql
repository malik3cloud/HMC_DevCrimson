CREATE    PROCEDURE [etl].[JobAuditComplete] (
	@JobAuditKey BIGINT
)
AS

DECLARE @LogMessage NVARCHAR(4000)
DECLARE @CurrentUTCDateTime DATETIME 
SET @CurrentUTCDateTime = GETUTCDATE()

-- if any tasks are still marked as running, change their running status to false and update the LastUpdated date
UPDATE etl.TaskAudit SET IsRunning = 0, LastUpdated = GETUTCDATE() WHERE JobAuditKey = @JobAuditKey AND IsRunning = 1

-- check for errors within any tasks and mark the job as failed if one or more tasks is in a non-Success state
DECLARE @TaskErrors INT

SELECT @TaskErrors = COUNT(*) FROM etl.TaskAudit WHERE JobAuditKey = @JobAuditKey AND Status != 'Success'

DECLARE @JobStatus NVARCHAR(100)

IF ISNULL(@TaskErrors, 0) = 0
	SET @JobStatus = 'Success'
ELSE
	SET @JobStatus = 'Failed'

UPDATE etl.JobAudit
SET 
	Status = @JobStatus,
	EndTime = @CurrentUTCDateTime
WHERE JobAuditKey = @JobAuditKey

SET @LogMessage = 'Job complete. Marked as "' + @JobStatus + '"'
EXEC etl.LogMessage @JobAuditKey = @JobAuditKey, @TaskAuditKey = NULL, @Message = @LogMessage
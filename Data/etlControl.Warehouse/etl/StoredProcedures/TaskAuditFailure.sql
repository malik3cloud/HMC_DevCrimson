CREATE   PROCEDURE [etl].[TaskAuditFailure] (
	@TaskAuditKey BIGINT,
	@Message NVARCHAR(4000) = NULL
)
AS

DECLARE @LogMessage NVARCHAR(4000)
DECLARE @CurrentUTCDateTime DATETIME 
SET @CurrentUTCDateTime = GETUTCDATE()  

SET @LogMessage = 'Task Failed. Check etl.TaskAudit or etl.Alert tables for details.'
EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage

EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @Message

-- only update DebugString if it's null. This ensures retention of original error message instead
-- of repacing the real error message with generic "inner activity failed"

UPDATE etl.TaskAudit
SET 
	IsRunning = 0,
	Status = 'Failed',
	LastUpdated = @CurrentUTCDateTime,
	EndTime = @CurrentUTCDateTime,
	DebugString = ISNULL(DebugString, @Message)
WHERE TaskAuditKey = @TaskAuditKey
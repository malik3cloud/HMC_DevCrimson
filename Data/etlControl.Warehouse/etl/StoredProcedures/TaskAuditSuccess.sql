CREATE     PROCEDURE [etl].[TaskAuditSuccess] (
	@TaskAuditKey BIGINT
)
AS
/*************************************************************************************************************************************************
PURPOSE:
High level structure of this stored procedure: 
1. Update TaskAudit status to Success
**************************************************************************************************************************************************/
DECLARE @LogMessage NVARCHAR(4000)
DECLARE @CurrentUTCDateTime DATETIME 
SET @CurrentUTCDateTime = GETUTCDATE()

SET @LogMessage = 'Task complete. Marked as "Success"'
EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage

UPDATE etl.TaskAudit
SET 
	IsRunning = 0,
	Status = 'Success',
	LastUpdated = @CurrentUTCDateTime,
	EndTime = @CurrentUTCDateTime,
    DebugString = NULL
WHERE TaskAuditKey = @TaskAuditKey
CREATE   PROCEDURE [etl].[TaskAuditStart] (
	@TaskAuditKey BIGINT
	)
AS
/******************************************************************************
PURPOSE: this proc will retrieves and generates various metadata to pass
to Fabric data pipeline to use as dynamic parameters in pipeline functionality
******************************************************************************/
BEGIN
SET NOCOUNT ON;

DECLARE @LogMessage NVARCHAR(4000)
DECLARE @CurrentUTCDateTime DATETIME 
SET @CurrentUTCDateTime = GETUTCDATE() 

DECLARE @SourceExtractionMethod NVARCHAR(40)
DECLARE @IsPartitionSeedingEnabledFlag BIT

DECLARE @taskAlreadyRunning BIT = 0
SET @taskAlreadyRunning = (SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM etl.TaskAudit WHERE TaskAuditKey = @TaskAuditKey AND IsRunning = 1)

IF @taskAlreadyRunning = 1
BEGIN
	DECLARE @ErrorMessage NVARCHAR(2048);
	SET @ErrorMessage =  'Error: TaskAuditKey "' + CONVERT(VARCHAR(5), @TaskAuditKey) + '" is already marked as running in the etl.TaskAudit table (IsRunning = 1)';
	EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @PipelineRunID = null, @Message = @ErrorMessage;

	THROW 50000, @ErrorMessage, 1;
END
ELSE
BEGIN
	DECLARE @TaskKey INT, @TaskType NVARCHAR(100)

	SELECT @TaskKey = TaskKey, @TaskType = TaskType
	FROM etl.TaskAudit
	WHERE TaskAuditKey = @TaskAuditKey;
		
	UPDATE etl.TaskAudit SET IsRunning = 1, StartTime = @CurrentUTCDateTime, Status = 'Execution started', 
		DebugString = NULL, LastUpdated = @CurrentUTCDateTime
	WHERE TaskAuditKey = @TaskAuditKey
	
	SET @LogMessage = 'Task execution started for ' + @TaskType + ' ' + CONVERT(NVARCHAR(5), @TaskKey)
	EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @PipelineRunID = null, @Message = @LogMessage

	-- return values
	SELECT
		@TaskKey AS TaskKey
	,	@TaskType AS TaskType
END

END
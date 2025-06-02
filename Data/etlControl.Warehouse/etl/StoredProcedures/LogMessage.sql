CREATE      PROCEDURE [etl].[LogMessage]
(
    @JobAuditKey BIGINT = NULL,
	@TaskAuditKey BIGINT = NULL,
	@PipelineRunID NVARCHAR(100) = NULL,
	@Message NVARCHAR(4000)
)
AS
SET @PipelineRunID = lower(@PipelineRunID)
DECLARE @PipelineRunID_string NVARCHAR(100)
SET @PipelineRunID_string = lower(convert(nvarchar(100),@PipelineRunID))

BEGIN
    SET NOCOUNT ON
	
	BEGIN

        DECLARE @CurrentUTCDateTime DATETIME 
        SET @CurrentUTCDateTime = GETUTCDATE()
		
        -- if a TaskAuditKey is supplied but JobAuditKey is null, we can check the etl.TaskAudit table to get the JobAuditKey. This is just a nice touch and not really critical.
		IF @JobAuditKey IS NULL AND @TaskAuditKey IS NOT NULL
            BEGIN
			    SET @JobAuditKey = (SELECT TOP 1 JobAuditKey FROM etl.TaskAudit WHERE TaskAuditKey = @TaskAuditKey)
            END

		DECLARE @MaxID AS BIGINT

		IF EXISTS(SELECT * FROM etl.Log)
			SET @MaxID = (SELECT MAX(LogKey) FROM etl.Log)
		ELSE
			SET @MaxID = 0

        INSERT INTO etl.Log (LogKey, JobAuditKey, TaskAuditKey, PipelineRunID, LogDatetime, Message)
        VALUES (@MaxID + 1, @JobAuditKey, @TaskAuditKey, @PipelineRunID_string, @CurrentUTCDateTime, @Message)
	END
END
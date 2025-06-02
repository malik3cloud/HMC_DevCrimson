CREATE    PROCEDURE [etl].[JobAuditFailure] (
	@JobAuditKey BIGINT
)
AS

DECLARE @CurrentUTCDateTime DATETIME 
SET @CurrentUTCDateTime = GETUTCDATE()

UPDATE etl.JobAudit
SET 
	Status = 'Failed',
	EndTime = @CurrentUTCDateTime
WHERE JobAuditKey = @JobAuditKey
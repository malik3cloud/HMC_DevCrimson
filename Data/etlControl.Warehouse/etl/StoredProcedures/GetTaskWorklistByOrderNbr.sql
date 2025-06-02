CREATE   PROCEDURE [etl].[GetTaskWorklistByOrderNbr] 
(
	@JobAuditKey BIGINT,
	@TaskRunOrderNbr BIGINT
)
AS

SELECT TaskAuditKey, TaskType
FROM etl.TaskAudit
WHERE JobAuditKey = @JobAuditKey 
AND TaskRunOrderNbr = @TaskRunOrderNbr
AND ISNULL(IsRunning, 0) = 0
AND ISNULL(Status, '') != 'Success'
ORDER BY TaskAuditKey
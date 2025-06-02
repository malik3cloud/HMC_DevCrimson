CREATE   PROCEDURE [etl].[GetTaskWorklist] 
(
	@JobAuditKey INT
)
AS

SELECT TaskAuditKey, TaskType
FROM etl.TaskAudit
WHERE JobAuditKey = @JobAuditKey AND ISNULL(IsRunning, 0) = 0
AND ISNULL(Status, '') != 'Success'
ORDER BY TaskRunOrderNbr
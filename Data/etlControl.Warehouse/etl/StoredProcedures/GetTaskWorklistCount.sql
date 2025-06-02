CREATE   PROCEDURE [etl].[GetTaskWorklistCount] 
(
	@JobAuditKey BIGINT,
	@TaskRunOrderNbr BIGINT
)
AS

SELECT COUNT(TaskAuditKey) AS  TaskAuditKeyCount
FROM etl.TaskAudit
WHERE JobAuditKey = @JobAuditKey 
AND TaskRunOrderNbr = @TaskRunOrderNbr
AND ISNULL(IsRunning, 0) = 0
AND ISNULL(Status, '') != 'Success'
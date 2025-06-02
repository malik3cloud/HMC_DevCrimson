CREATE   PROCEDURE [etl].[GetTaskRunOrderNbr] 
(
	@JobAuditKey INT
)
AS

SELECT TaskRunOrderNbr
FROM etl.TaskAudit
WHERE JobAuditKey = @JobAuditKey AND ISNULL(IsRunning, 0) = 0
AND ISNULL(Status, '') != 'Success'
GROUP BY TaskRunOrderNbr
ORDER BY TaskRunOrderNbr
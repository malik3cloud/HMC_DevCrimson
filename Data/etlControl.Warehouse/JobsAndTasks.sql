
/*
select * from  etl.Job order by JobKey
select * from etl.Task 
select * from etl.FileTask
*/


SELECT j.JobKey, t.TaskKey, j.JobName, t.TaskRunOrderNbr, t.TaskName, t.TaskType, t.TaskTableName
FROM etl.Job j
JOIN (
    SELECT JobKey, TaskKey, TaskName, TaskType, TaskRunOrderNbr, 'Task' as 'TaskTableName' FROM etl.Task
    UNION ALL
    SELECT JobKey, FileTaskKey as TaskKey, TaskName, TaskType, TaskRunOrderNbr, 'FileTask' as 'TaskTableName' FROM etl.FileTask
) AS t ON j.JobKey = t.JobKey
order by JobKey, TaskName


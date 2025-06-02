CREATE    PROCEDURE [etl].[GetApiTaskNotebooks]
(
	@TaskKey INT
)
AS

/******************************************************************************
PURPOSE: This procedure is to gather and generate metadata to be used in 
Data Factory data pipeline as dynamic parameters to processing API data ingestion
******************************************************************************/

SELECT 
	Task.NotebookKey AS NotebookKey, 
	Notebook.NotebookName AS NotebookName
FROM etl.Task
-- CROSS APPLY STRING_SPLIT(etl.Task.NotebookKeys, ',') 
LEFT JOIN etl.Notebook
ON Task.NotebookKey = Notebook.NotebookKey
WHERE Task.TaskKey = @TaskKey
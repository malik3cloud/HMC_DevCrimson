CREATE    PROCEDURE [etl].[GetFileTaskNotebooks]
(
	@TaskKey INT
)
AS

/******************************************************************************
PURPOSE: This procedure is to gather and generate metadata to be used in 
Data Factory data pipeline as dynamic parameters to processing API data ingestion
******************************************************************************/

SELECT 
	FileTask.NotebookKey AS NotebookKey, 
	Notebook.NotebookName AS NotebookName
FROM etl.FileTask
-- CROSS APPLY STRING_SPLIT(etl.Task.NotebookKeys, ',') 
LEFT JOIN etl.Notebook
ON FileTask.NotebookKey = Notebook.NotebookKey
WHERE FileTask.FileTaskKey = @TaskKey
CREATE PROCEDURE [etl].[PopulateWatermark]
AS
BEGIN
    SET NOCOUNT ON;

    -- Insert TaskKey-only rows (FileTaskKey = NULL)
    INSERT INTO etl.Watermark (TaskKey, FileTaskKey)
    SELECT t.TaskKey, NULL
    FROM etl.Task t
    LEFT JOIN etl.Watermark w ON w.TaskKey = t.TaskKey AND w.FileTaskKey IS NULL
    WHERE 
	--t.IsWatermarkEnabledFlag = 1
      --AND 
	  w.TaskKey IS NULL;

    -- Insert FileTaskKey-only rows (TaskKey = NULL)
    INSERT INTO etl.Watermark (TaskKey, FileTaskKey)
    SELECT NULL, ft.FileTaskKey
    FROM etl.FileTask ft
    LEFT JOIN etl.Watermark w ON w.FileTaskKey = ft.FileTaskKey AND w.TaskKey IS NULL
    WHERE w.FileTaskKey IS NULL;
END;
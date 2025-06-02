CREATE PROCEDURE [etl].[TaskAuditQueue] (
    @TaskAuditJSON NVARCHAR(MAX),  
    @StartTime DATETIME,
    @AdditionalMessage NVARCHAR(MAX)  = ''
)
------------------------------
-- Add task audit in stage --
-- Add watermark in log    --
------------------------------
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @NewLogID INT;
    DECLARE @ExistingHighWatermark VARCHAR(200);
    DECLARE @ExistingVersionLoaded INT;

    -- Get the latest watermark values for this TaskKey
    SELECT TOP 1 
        @ExistingHighWatermark = HighWatermarkValue,
        @ExistingVersionLoaded = VersionLoaded
    FROM etl.WatermarkLog
    WHERE TaskKey = JSON_VALUE(@TaskAuditJSON, '$[0].TaskKey')  -- Extract TaskKey from first JSON entry
    ORDER BY LogID DESC;  -- Get latest entry

    -- Use COALESCE to keep the existing value if NULL is passed
    DECLARE @HighWatermarkValue VARCHAR(200) = COALESCE(JSON_VALUE(@TaskAuditJSON, '$[0].Watermark'), @ExistingHighWatermark);
    DECLARE @VersionLoaded INT = COALESCE(JSON_VALUE(@TaskAuditJSON, '$[0].VersionLoaded'), @ExistingVersionLoaded);

    -- Generate the next LogID safely
    SELECT @NewLogID = COALESCE((SELECT MAX(LogID) FROM etl.WatermarkLog), 0) + 1;

    -- Directly parse JSON and insert into etl.TaskAuditStage
    INSERT INTO etl.TaskAuditStage (
        TaskAuditLogKey, TaskAuditKey, TaskKey, 
        RequestedStartTime, Processed, RowsRead, RowsUpdated, RowsInserted, 
        RowsDeleted, LogMessage, RequestedStatus, RequestedEndTime
    )
    SELECT 
        JSON_VALUE(t.value, '$.TaskAuditKey') AS TaskAuditLogKey,
        JSON_VALUE(t.value, '$.TaskAuditKey') AS TaskAuditKey,
        JSON_VALUE(t.value, '$.TaskKey') AS TaskKey,
        @StartTime AS RequestedStartTime,
        1 AS Processed,
        JSON_VALUE(t.value, '$.RowsRead') AS RowsRead,
        JSON_VALUE(t.value, '$.RowsUpdated') AS RowsUpdated,
        JSON_VALUE(t.value, '$.RowsInserted') AS RowsInserted,
        JSON_VALUE(t.value, '$.RowsDeleted') AS RowsDeleted,
        JSON_VALUE(t.value, '$.Message') + '|' + @AdditionalMessage AS LogMessage,
        ISNULL(NULLIF(JSON_VALUE(t.value, '$.Status'), ''), '') AS RequestedStatus, -- Ensure empty string if NULL or empty
        ISNULL(NULLIF(JSON_VALUE(t.value, '$.RequestedEndTime'), ''), '') AS RequestedEndTime  -- Ensure empty string if NULL or empty
    FROM OPENJSON(@TaskAuditJSON) AS t;

    -- Insert into etl.WatermarkLog
    INSERT INTO etl.WatermarkLog (LogID, TaskKey, HighWatermarkValue, VersionLoaded)
    SELECT 
        @NewLogID,
        JSON_VALUE(t.value, '$.TaskKey'),
        JSON_VALUE(t.value, '$.Watermark'),
        JSON_VALUE(t.value, '$.VersionLoaded')
    FROM OPENJSON(@TaskAuditJSON) AS t
    WHERE JSON_VALUE(t.value, '$.Watermark') IS NOT NULL;  -- Ensure only valid watermark values are inserted
END;
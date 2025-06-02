CREATE PROCEDURE [etl].[ProcessTaskAuditUpdates]
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRANSACTION;
    
    DECLARE @CurrentUTCDateTime DATETIME;
    SET @CurrentUTCDateTime = GETUTCDATE();

    -- Aggregate log entries per TaskAuditKey before updating TaskAudit
    WITH CombinedLog AS (
        SELECT 
            TaskAuditKey,
            MAX(RequestedStartTime) AS RequestedStartTime,
            STRING_AGG(LogMessage, ' | ') AS DebugString,
            MAX(RowsRead) AS RowsRead,
            MAX(RowsUpdated) AS RowsUpdated,
            MAX(RowsInserted) AS RowsInserted,
            MAX(RowsDeleted) AS RowsDeleted,
            STRING_AGG(RequestedStatus,'') AS RequestedStatus,
            MAX(RequestedEndTime) AS RequestedEndTime
        FROM etl.TaskAuditStage
        GROUP BY TaskAuditKey
    )
    
    -- Update TaskAudit with combined values
    UPDATE TA
    SET 
        Status = CL.RequestedStatus,
        StartTime = CL.RequestedStartTime,
        DebugString = CL.DebugString,
        RowsRead = CL.RowsRead,
        RowsUpdated = CL.RowsUpdated,
        RowsInserted = CL.RowsInserted,
        RowsDeleted = CL.RowsDeleted,
        LastUpdated = @CurrentUTCDateTime,
        EndTime = CL.RequestedEndTime
    FROM etl.TaskAudit TA
    INNER JOIN CombinedLog CL 
        ON TA.TaskAuditKey = CL.TaskAuditKey;

    -- Mark processed log entries
    UPDATE etl.TaskAuditStage
    SET Processed = 1, ProcessedTime = @CurrentUTCDateTime
    WHERE Processed = 0;

    DELETE FROM etl.TaskAuditStage WHERE ProcessedTime < @CurrentUTCDateTime



    COMMIT TRANSACTION;
END;
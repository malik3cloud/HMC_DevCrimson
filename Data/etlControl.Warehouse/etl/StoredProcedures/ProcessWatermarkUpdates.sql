CREATE PROCEDURE etl.ProcessWatermarkUpdates
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRANSACTION;

    BEGIN TRY
        -- Step 1: Update existing TaskKeys in etl.Watermark
        UPDATE etl.Watermark
        SET HighWatermarkValue = (
            SELECT TOP 1 CONVERT(VARCHAR(30), l.HighWatermarkValue, 127)  -- Force ISO 8601 format
            FROM etl.WatermarkLog l
            WHERE l.TaskKey = etl.Watermark.TaskKey
            ORDER BY l.HighWatermarkValue DESC  -- Get latest value
        ),
        LastVersionLoaded = (
            SELECT TOP 1 l.VersionLoaded
            FROM etl.WatermarkLog l
            WHERE l.TaskKey = etl.Watermark.TaskKey
            ORDER BY l.VersionLoaded DESC  -- Get latest version
        )
        WHERE EXISTS (
            SELECT 1 FROM etl.WatermarkLog l WHERE l.TaskKey = etl.Watermark.TaskKey
        );

        -- Step 2: Insert new TaskKeys that don't exist in etl.Watermark
        INSERT INTO etl.Watermark (TaskKey, HighWatermarkValue, LastVersionLoaded)
        SELECT l.TaskKey, 
               CONVERT(VARCHAR(30), MAX(l.HighWatermarkValue), 127),  -- Ensure ISO format
               MAX(l.VersionLoaded)
        FROM etl.WatermarkLog l
        LEFT JOIN etl.Watermark w ON l.TaskKey = w.TaskKey
        WHERE w.TaskKey IS NULL
        GROUP BY l.TaskKey;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        THROW;
    END CATCH;
END;
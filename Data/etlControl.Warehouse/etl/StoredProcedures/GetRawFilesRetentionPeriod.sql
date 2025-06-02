CREATE    PROCEDURE [etl].[GetRawFilesRetentionPeriod]
AS

/******************************************************************************
PURPOSE: This procedure is to get raw files retention period.
Defaults to 30 days
******************************************************************************/

SELECT ISNULL(ConfigValue, '30') AS RetentionPeriod 
FROM etl.GlobalConfig 
WHERE ConfigKey = 'RawFilesRetentionPeriodDays'
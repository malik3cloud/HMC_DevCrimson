CREATE    PROCEDURE [etl].[GetApiTaskInfo]
(
	@TaskKey INT, @TaskAuditKey BIGINT
)
AS
BEGIN
/******************************************************************************
PURPOSE: This procedure is to gather and generate metadata to be used in 
Data Factory data pipeline as dynamic parameters to processing API data ingestion
******************************************************************************/

SET NOCOUNT ON
DECLARE @JobAuditKey BIGINT
DECLARE @TaskType NVARCHAR(40)
DECLARE @ParentSourceName NVARCHAR(200)
DECLARE @SourceName NVARCHAR(200)
DECLARE @ApiTaskName NVARCHAR(2000)
DECLARE @PrimaryKeyColumnList  NVARCHAR(1000)
DECLARE @IsWatermarkEnabledFlag BIT -- (optional) enables incremental-loading functionality
DECLARE @SourceWhereClause NVARCHAR(1000)
DECLARE @LowerLimit NVARCHAR(200) -- (optional but required for incremental) 
DECLARE @UpperLimit NVARCHAR(200) -- (optional but required for incremental) 
DECLARE @LimitType NVARCHAR(200) -- (optional but required for incremental) 
DECLARE @Lower DATETIME
DECLARE @Upper DATETIME

-- variables for raw,bronze,archive processing
DECLARE @RawStoragePath NVARCHAR(2000)
DECLARE @RawStorageFileName NVARCHAR(2000)

DECLARE @ArchiveOriginalFilesFlag BIT
DECLARE @ArchiveStoragePath NVARCHAR(2000);
DECLARE @ArchiveStorageFileName NVARCHAR(2000);

DECLARE @BronzeWorkspaceName NVARCHAR(2000)
DECLARE @BronzeLakehouseName NVARCHAR(2000)
DECLARE @BronzeSchemaName NVARCHAR(2000)
DECLARE @BronzeTableName NVARCHAR(2000)
DECLARE @BronzeLoadMethod NVARCHAR(40)

-- vars not returned
DECLARE @LogMessage NVARCHAR(4000)
DECLARE @errorMessage NVARCHAR(2048) 

-- Get TaskAudit info
-- Note: limits are already set in SetLimitAuditInfo proc called in pipeline before this proc
SELECT 
    @TaskType = TaskType,
    @JobAuditKey = JobAuditKey
FROM etl.TaskAudit
WHERE TaskAuditKey = @TaskAuditKey;

SELECT TOP 1
     @ParentSourceName = ApiTask.ParentSourceName
    ,@SourceName = ApiTask.SourceName
    ,@ApiTaskName = ApiTask.TaskName
    ,@PrimaryKeyColumnList = ApiTask.PrimaryKeyColumnList
    ,@RawStoragePath = (SELECT TOP 1 COALESCE(ApiTask.RawStoragePath, ConfigValue) from etl.GetGlobalConfig('ApiTask_RawStoragePath'))
    ,@RawStorageFileName = ApiTask.RawStorageFileName

	,@ArchiveOriginalFilesFlag = ApiTask.ArchiveOriginalFilesFlag
    ,@ArchiveStoragePath = (SELECT TOP 1 COALESCE(ApiTask.ArchiveStoragePath, ConfigValue) from etl.GetGlobalConfig('ApiTask_ArchiveStoragePath'))
    ,@ArchiveStorageFileName = ISNULL(ApiTask.ArchiveStorageFileName, ApiTask.RawStorageFileName)

	,@BronzeWorkspaceName = ApiTask.SinkWorkspaceName
    ,@BronzeLakehouseName = ApiTask.SinkLakehouseName
	,@BronzeSchemaName = ApiTask.SinkSchemaName
    ,@BronzeTableName = ApiTask.SinkTableName
    ,@BronzeLoadMethod = ApiTask.SinkLoadMethod
    ,@IsWatermarkEnabledFlag = ApiTask.IsWatermarkEnabledFlag
    ,@SourceWhereClause = Watermark.SourceWhereClause
    ,@LowerLimit = Watermark.LowerLimit
    ,@UpperLimit = Watermark.UpperLimit
    ,@LimitType = Watermark.LimitType
FROM etl.Task as ApiTask
LEFT JOIN etl.Watermark
ON ApiTask.TaskKey = Watermark.TaskKey
WHERE ApiTask.TaskKey = @TaskKey;

-- replace string tokens
IF CHARINDEX('{', @RawStoragePath) > 0
BEGIN
	SET @RawStoragePath = REPLACE(REPLACE(@RawStoragePath, '{SourceName}', @SourceName), '{TaskName}', @ApiTaskName)
END

IF CHARINDEX('{', @ArchiveStoragePath) > 0
BEGIN
	SET @ArchiveStoragePath = REPLACE(REPLACE(@ArchiveStoragePath, '{SourceName}', @SourceName), '{TaskName}', @ApiTaskName)
END

-- convert incremental limit datatypes to standard DATETIME to return to pipeline
-- these may be used by notebook and should all be the same format for ease of use
IF @IsWatermarkEnabledFlag = 1
BEGIN
    SELECT
        @Lower = CASE 
                WHEN @LimitType = 'Integer Date' THEN TRY_CONVERT(DATETIME, @LowerLimit, 112) 
                WHEN @LimitType = 'DateTime' THEN TRY_CONVERT(DATETIME, @LowerLimit, 120) 
                WHEN @LimitType = 'Date' THEN TRY_CONVERT(DATETIME, @LowerLimit) 
                ELSE CAST(NULL AS DATETIME) 
                END,
        @Upper = CASE 
                WHEN @LimitType = 'Integer Date' THEN TRY_CONVERT(DATETIME, @UpperLimit, 112)
                WHEN @LimitType = 'DateTime' THEN TRY_CONVERT(DATETIME, @UpperLimit, 120) 
                WHEN @LimitType = 'Date' THEN TRY_CONVERT(DATETIME, @UpperLimit)
                ELSE CAST(NULL AS DATETIME) 
                END
END

SET @LogMessage = 'From etl.GetApiTaskInfo: ParentSourceName = "' + ISNULL(@ParentSourceName, '(NULL)') + '"'
EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
SET @LogMessage = 'From etl.GetApiTaskInfo: SourceName = "' + ISNULL(@SourceName, '(NULL)') + '"'
EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
SET @LogMessage = 'From etl.GetApiTaskInfo: ApiTaskName = "' + ISNULL(@ApiTaskName, '(NULL)') + '"'
EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
SET @LogMessage = 'From etl.GetApiTaskInfo: TaskType = "' + ISNULL(@TaskType, '(NULL)') + '"'
EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
SET @LogMessage = 'From etl.GetApiTaskInfo: PrimaryKeyColumnList = "' + ISNULL(@PrimaryKeyColumnList, '(NULL)') + '"'
EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
-- raw logs
SET @LogMessage = 'From etl.GetApiTaskInfo: RawStoragePath = "' + ISNULL(@RawStoragePath, '(NULL)') + '"'
EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
SET @LogMessage = 'From etl.GetApiTaskInfo: RawStorageFileName = "' + ISNULL(@RawStorageFileName, '(NULL)') + '"'
EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
-- archive logs
SET @LogMessage = 'From etl.GetApiTaskInfo: ArchiveOriginalFilesFlag = "' + ISNULL(CAST(@ArchiveOriginalFilesFlag AS VARCHAR(1)), '(NULL)') + '"'
EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
SET @LogMessage = 'From etl.GetApiTaskInfo: ArchiveStoragePath = "' + ISNULL(@ArchiveStoragePath, '(NULL)') + '"'
EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
SET @LogMessage = 'From etl.GetApiTaskInfo: ArchiveStorageFileName = "' + ISNULL(@ArchiveStorageFileName, '(NULL)') + '"'
EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
-- bronze logs
SET @LogMessage = 'From etl.GetApiTaskInfo: BronzeWorkspaceName = "' + ISNULL(@BronzeWorkspaceName, '(NULL)') + '"'
EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
SET @LogMessage = 'From etl.GetApiTaskInfo: BronzeLakehouseName = "' + ISNULL(@BronzeLakehouseName, '(NULL)') + '"'
EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
SET @LogMessage = 'From etl.GetApiTaskInfo: BronzeTableName = "' + ISNULL(@BronzeTableName, '(NULL)') + '"'
EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
SET @LogMessage = 'From etl.GetApiTaskInfo: BronzeLoadMethod = "' + ISNULL(@BronzeLoadMethod, '(NULL)') + '"'
EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage

SET @LogMessage = 'From etl.GetApiTaskInfo: IsWatermarkEnabledFlag = "' + ISNULL(CAST(@IsWatermarkEnabledFlag AS VARCHAR(1)), '(NULL)') + '"'
EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
SET @LogMessage = 'From etl.GetApiTaskInfo: LowerLimit = "' + ISNULL(@LowerLimit, '(NULL)') + '"'
EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
SET @LogMessage = 'From etl.GetApiTaskInfo: UpperLimit = "' + ISNULL(@UpperLimit, '(NULL)') + '"'
EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
SET @LogMessage = 'From etl.GetApiTaskInfo: LimitType = "' + ISNULL(@LimitType, '(NULL)') + '"'
EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage

	-- Return metadata
	SELECT
        @JobAuditKey as JobAuditKey,
        @TaskKey as TaskKey,
        @TaskType as TaskType,
        @ParentSourceName as ParentSourceName,
       	@SourceName as SourceName,
        @ApiTaskName as ApiTaskName,
        @PrimaryKeyColumnList as PrimaryKeyColumnList,
        @IsWatermarkEnabledFlag as IsWatermarkEnabledFlag,
        @SourceWhereClause as SourceWhereClause,
        @Lower as LowerLimit,
        @Upper as UpperLimit,
        @LimitType as LimitType,
        @ArchiveOriginalFilesFlag as ArchiveOriginalFilesFlag,
		@ArchiveStoragePath as ArchiveStoragePath,
		@ArchiveStorageFileName as ArchiveStorageFileName,
        @RawStoragePath as RawStoragePath,
        @RawStorageFileName as RawStorageFileName,
		@BronzeWorkspaceName as BronzeWorkspaceName,
        @BronzeLakehouseName as BronzeLakehouseName,
		@BronzeSchemaName as BronzeSchemaName,
		@BronzeTableName as BronzeTableName,
        @BronzeLoadMethod as BronzeLoadMethod

END
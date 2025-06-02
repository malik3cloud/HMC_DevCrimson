-- =============================================
-- PURPOSE: this proc will retrieves and generates various metadata to pass
-- to Data factory pipeline to use as dynamic parameters in pipeline functionality
-- =============================================
CREATE PROCEDURE [etl].[GetFileTaskInfo]
(
	@TaskAuditKey BIGINT
)
AS
BEGIN

	SET NOCOUNT ON;

-- utility variables
DECLARE @LogMessage NVARCHAR(4000)
DECLARE @errorMessage NVARCHAR(2048) -- just declare this here. there are several spots later where we may attempt to use this
DECLARE @FileTaskKey INT;
DECLARE @JobAuditKey INT;
DECLARE @TaskType NVARCHAR(20);
DECLARE @SourceName NVARCHAR(200);

-- temp variables for raw,bronze,archive processing
DECLARE @RawLakehouseName NVARCHAR(2000)
DECLARE @RawStoragePath NVARCHAR(2000)
DECLARE @RawStorageFileName NVARCHAR(2000)
DECLARE @SourceTableName NVARCHAR(2000)
DECLARE @Delimiter NVARCHAR(2000)

DECLARE @ETLWarehouseName NVARCHAR(2000)

DECLARE @ArchiveOriginalFilesFlag BIT
DECLARE @ArchiveStoragePath NVARCHAR(2000);
DECLARE @ArchiveStorageFileName NVARCHAR(2000);

DECLARE @BronzeWorkspaceName NVARCHAR(2000)
DECLARE @BronzeLakehouseName NVARCHAR(2000)
DECLARE @BronzeSchemaName NVARCHAR(2000)
DECLARE @BronzeObjectName NVARCHAR(2000)
DECLARE @BronzeObjectType NVARCHAR(2000)
DECLARE @BronzeLoadMethod NVARCHAR(40)
DECLARE @SinkLoadMethod NVARCHAR(40)
DECLARE @BronzeTableName NVARCHAR(2000)
DECLARE @WatermarkColumn NVARCHAR(2000)
DECLARE @SinkWatermarkColumn NVARCHAR(2000)
DECLARE @SinkSchemaName NVARCHAR(2000)
DECLARE @SinkTableName NVARCHAR(2000)
DECLARE @SkipRows INT
DECLARE @SinkWorkspaceName NVARCHAR(2000)

-- declare the additional metadata
DECLARE @SourceType NVARCHAR(50)
DECLARE @FileType NVARCHAR(50)

DECLARE @SourceWildcardFolderPath NVARCHAR(200)
DECLARE @SourceWildcardFileName NVARCHAR(200)
DECLARE @SourceDataSet NVARCHAR(200)
DECLARE @PrimaryKeyColumnList NVARCHAR(1000)

DECLARE @IsWatermarkEnabledFlag BIT
DECLARE @IsPartitionSeedingEnabledFlag BIT
DECLARE @SourceFullExtractOverrideFlag BIT

	-- Logic
	SELECT @FileTaskKey = TaskKey
	FROM etl.TaskAudit
	WHERE TaskAuditKey = @TaskAuditKey;

	-- validate TaskAuditKey and throw error if invalid as rest of proc will not work properly
IF NOT EXISTS (SELECT TOP 1 TaskKey FROM etl.TaskAudit WHERE TaskAuditKey = @TaskAuditKey)
BEGIN
	SET @errorMessage =  'Error: Lookup to etl.TaskAudit failed for TaskAuditKey: "' + ISNULL(CONVERT(VARCHAR(20), @TaskAuditKey), 'NULL') + '" Please verify the TaskAuditKey parameter passed to proc etl.GetFileTaskInfo.';
	THROW 50000, @errorMessage, 1;
END

SELECT 
	@FileTaskKey = TaskKey,
	@TaskType = TaskType,
	@JobAuditKey = JobAuditKey
FROM etl.TaskAudit
WHERE TaskAuditKey = @TaskAuditKey


-- get Source and FileTask info
SELECT TOP 1
	 @SourceName = FileTask.SourceName
	,@SourceType = FileTask.SourceType
	,@FileType = FileTask.FileType
	
	,@SourceWildcardFolderPath = FileTask.SourceWildcardFolderPath
	,@SourceWildcardFileName = FileTask.SourceWildcardFileName
	,@SourceDataSet = FileTask.SourceDataSet
	,@PrimaryKeyColumnList = FileTask.PrimaryKeyColumnList
	,@RawLakehouseName = FileTask.RawLakehouseName
	,@RawStoragePath = FileTask.RawStoragePath
	,@RawStorageFileName = FileTask.RawStorageFileName
	,@SourceTableName = FileTask.RawStorageFileName
	,@Delimiter = FileTask.Delimiter

	,@ETLWarehouseName = FileTask.ETLWarehouseName

	--,@ArchiveOriginalFilesFlag = FileTask.ArchiveOriginalFilesFlag
	--,@ArchiveStoragePath = (SELECT TOP 1 COALESCE(FileTask.ArchiveStoragePath, ConfigValue) from etl.GetGlobalConfig('FileTask_ArchiveStoragePath'))
 --   ,@ArchiveStorageFileName = ISNULL(FileTask.ArchiveStorageFileName,  FileTask.RawStorageFileName) 

	,@BronzeWorkspaceName = FileTask.SinkWorkspaceName
	,@BronzeLakehouseName = FileTask.SinkLakehouseName
    ,@BronzeSchemaName = FileTask.SinkSchemaName
    ,@SinkTableName = FileTask.SinkTableName
	,@SinkSchemaName = FileTask.SinkSchemaName
    ,@BronzeTableName = FileTask.SinkTableName
	,@BronzeObjectType = FileTask.SinkObjectType
    ,@BronzeLoadMethod = FileTask.SinkLoadMethod
	,@SinkLoadMethod = FileTask.SinkLoadMethod
    ,@WatermarkColumn = FileTask.SinkWatermarkColumn
	,@SinkWatermarkColumn = FileTask.SinkWatermarkColumn
	,@SkipRows = FileTask.SkipRows
	,@SinkWorkspaceName = FileTask.SinkWorkspaceName

	,@IsWatermarkEnabledFlag = FileTask.IsWatermarkEnabledFlag
	--,@SourceFullExtractOverrideFlag = FileTask.SourceFullExtractOverrideFlag

FROM etl.FileTask as FileTask
WHERE FileTask.FileTaskKey = @FileTaskKey
;


BEGIN
	SET @LogMessage = 'From etl.GetFileTaskInfo: SourceName = "' + ISNULL(@SourceName, '(NULL)') + '"'
	EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	SET @LogMessage = 'From etl.GetFileTaskInfo: SourceType = "' + ISNULL(@sourceType, '(NULL)') + '"'
	EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	SET @LogMessage = 'From etl.GetFileTaskInfo: SourceType = "' + ISNULL(@fileType, '(NULL)') + '"'
	EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	SET @LogMessage = 'From etl.GetFileTaskInfo: SourceDatabaseName = "' + ISNULL(@SourceWildcardFolderPath, '(NULL)') + '"'
	EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	SET @LogMessage = 'From etl.GetFileTaskInfo: SourceWildcardFileName = "' + ISNULL(@SourceWildcardFileName, '(NULL') + '"'
	EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	SET @LogMessage = 'From etl.GetFileTaskInfo: SourceDataSet = "' + ISNULL(@SourceDataSet, '(NULL)') + '"'
	EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	SET @LogMessage = 'From etl.GetFileTaskInfo: PrimaryKeyColumnList = "' + ISNULL(@primaryKeyColumnList, '(NULL)') + '"'
	EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	SET @LogMessage = 'From etl.GetFileTaskInfo: RawLakehouseName = "' + ISNULL(@RawLakehouseName, '(NULL)') + '"'
	EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	SET @LogMessage = 'From etl.GetFileTaskInfo: RawStoragePath = "' + ISNULL(@RawStoragePath, '(NULL)') + '"'
	EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	SET @LogMessage = 'From etl.GetFileTaskInfo: ETLWarehouseName = "' + ISNULL(@ETLWarehouseName, '(NULL)') + '"'
	EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	SET @LogMessage = 'From etl.GetFileTaskInfo: RawStorageFileName = "' + ISNULL(@RawStorageFileName, '(NULL)') + '"'
	EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	--SET @LogMessage = 'From etl.GetFileTaskInfo: ArchiveOriginalFilesFlag = "' + ISNULL(CAST(@ArchiveOriginalFilesFlag AS VARCHAR(1)), '(NULL)') + '"'
	--EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	--SET @LogMessage = 'From etl.GetFileTaskInfo: ArchiveStoragePath = "' + ISNULL(@ArchiveStoragePath, '(NULL)') + '"'
	--EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	--SET @LogMessage = 'From etl.GetFileTaskInfo: ArchiveStorageFileName = "' + ISNULL(@ArchiveStorageFileName, '(NULL') + '"'
	--EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	SET @LogMessage = 'From etl.GetFileTaskInfo: BronzeWorkspaceName = "' + ISNULL(@BronzeWorkspaceName, '(NULL)') + '"'
    EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	SET @LogMessage = 'From etl.GetFileTaskInfo: BronzeLakehouseName = "' + ISNULL(@BronzeLakehouseName, '(NULL)') + '"'
    EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	SET @LogMessage = 'From etl.GetFileTaskInfo: WatermarkColumn = "' + ISNULL(@WatermarkColumn, '(NULL)') + '"'
	EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	SET @LogMessage = 'From etl.GetFileTaskInfo: BronzeSchemaName = "' + ISNULL(@BronzeSchemaName, '(NULL)') + '"'
	EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	SET @LogMessage = 'From etl.GetFileTaskInfo: BronzeObjectName = "' + ISNULL(@BronzeObjectName, '(NULL') + '"'
	EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	SET @LogMessage = 'From etl.GetFileTaskInfo: BronzeTypeName = "' + ISNULL(@BronzeObjectType, '(NULL') + '"'
	EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	SET @LogMessage = 'From etl.GetFileTaskInfo: BronzeLoadMethod = "' + ISNULL(@BronzeLoadMethod, '(NULL)') + '"'
	EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	SET @LogMessage = 'From etl.GetFileTaskInfo: IsWatermarkEnabledFlag = "' + ISNULL(CAST(@IsWatermarkEnabledFlag AS VARCHAR(1)), '(NULL)') + '"'
	EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
END

-- now return the metadata
SELECT
@JobAuditKey as JobAuditKey
,@FileTaskKey AS TaskKey
,@TaskType as TaskType
,@SourceName AS SourceName
,@SourceType AS SourceType
,@FileType AS FileType

,@SourceWildcardFolderPath AS SourceWildcardFolderPath
,@SourceWildcardFileName AS SourceWildcardFileName
,@SourceDataSet AS SourceDataSet
,ISNULL(@PrimaryKeyColumnList, '') AS PrimaryKeyColumnList
,@Delimiter AS Delimiter

,@RawLakehouseName AS RawLakehouseName
,@RawStoragePath AS RawStoragePath
,@RawStorageFileName AS RawStorageFileName

,@ETLWarehouseName AS ETLWarehouseName

--,@ArchiveOriginalFilesFlag AS ArchiveOriginalFilesFlag
--,@ArchiveStoragePath AS ArchiveStoragePath
--,@ArchiveStorageFileName as ArchiveStorageFileName

,@BronzeWorkspaceName AS BronzeWorkspaceName
,@BronzeLakehouseName AS BronzeLakehouseName
,@BronzeSchemaName AS BronzeSchemaName
,@BronzeObjectName AS BronzeObjectName
,@BronzeObjectType AS BronzeObject
,@BronzeLoadMethod AS BronzeLoadMethod
,@WatermarkColumn AS WatermarkColumn
,@SinkTableName AS SinkTableName
,@SinkSchemaName AS SinkSchemaName
,@SourceTableName AS SourceTableName
,@SinkWatermarkColumn AS SinkWatermarkColumn
,@SinkLoadMethod AS SinkLoadMethod

,@IsWatermarkEnabledFlag AS IsWatermarkEnabledFlag
,@SourceFullExtractOverrideFlag AS SourceFullExtractOverrideFlag
,@SkipRows AS SkipRows
,@SinkWorkspaceName AS SinkWorkspaceName
END
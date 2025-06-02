CREATE   PROCEDURE [etl].[GetDatabaseTaskInfo] 
	@TaskAuditKey BIGINT
AS
BEGIN
/******************************************************************************
PURPOSE: this proc will retrieves and generates various metadata to pass
to Data factory pipeline to use as dynamic parameters in pipeline functionality
******************************************************************************/
SET NOCOUNT ON;

-- utility variables
DECLARE @LogMessage NVARCHAR(4000)
DECLARE @errorMessage NVARCHAR(2048) -- just declare this here. there are several spots later where we may attempt to use this
DECLARE @TaskKey INT;
DECLARE @JobAuditKey INT;
DECLARE @TaskType NVARCHAR(20);
DECLARE @SourceName NVARCHAR(200);

-- temp variables for raw,bronze,archive processing
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
DECLARE @SinkLoadMethod NVARCHAR(40)
DECLARE @BronzeWorkspaceId NVARCHAR(2000)
DECLARE @BronzeLakehouseId NVARCHAR(2000)
DECLARE @WatermarkColumn NVARCHAR(2000)
DECLARE @SinkWatermarkColumn NVARCHAR(2000)
DECLARE @SinkSchemaName NVARCHAR(2000)
DECLARE @SinkTableName NVARCHAR(2000)

-- declare the additional metadata
DECLARE @SourceType NVARCHAR(50)

DECLARE @SourceDatabaseName NVARCHAR(200)
DECLARE @SourceSchemaName NVARCHAR(200)
DECLARE @SourceTableName NVARCHAR(200)
DECLARE @PrimaryKeyColumnList NVARCHAR(1000)

DECLARE @IsWatermarkEnabledFlag BIT
DECLARE @IsPartitionSeedingEnabledFlag BIT

-- validate TaskAuditKey and throw error if invalid as rest of proc will not work properly
IF NOT EXISTS (SELECT TOP 1 TaskKey FROM etl.TaskAudit WHERE TaskAuditKey = @TaskAuditKey)
BEGIN
	SET @errorMessage =  'Error: Lookup to etl.TaskAudit failed for TaskAuditKey: "' + ISNULL(CONVERT(VARCHAR(20), @TaskAuditKey), 'NULL') + '" Please verify the TaskAuditKey parameter passed to proc etl.GetDatabaseTaskInfo.';
	THROW 50000, @errorMessage, 1;
END

SELECT 
	@TaskKey = TaskKey,
	@TaskType = TaskType,
	@JobAuditKey = JobAuditKey
FROM etl.TaskAudit
WHERE TaskAuditKey = @TaskAuditKey

-- get Source and DatabaseTask info
SELECT TOP 1
	 @SourceName = DatabaseTask.SourceName
	,@SourceType = DatabaseTask.SourceType
	
	,@SourceDatabaseName = DatabaseTask.SourceDatabaseName
	,@SourceSchemaName = DatabaseTask.SourceSchemaName
	,@SourceTableName = DatabaseTask.SourceTableName
	,@PrimaryKeyColumnList = DatabaseTask.PrimaryKeyColumnList

	,@RawStoragePath = DatabaseTask.RawStoragePath
	,@RawStorageFileName = DatabaseTask.RawStorageFileName

	,@ArchiveOriginalFilesFlag = DatabaseTask.ArchiveOriginalFilesFlag
	,@ArchiveStoragePath = (SELECT TOP 1 COALESCE(DatabaseTask.ArchiveStoragePath, ConfigValue) from etl.GetGlobalConfig('DatabaseTask_ArchiveStoragePath'))
    ,@ArchiveStorageFileName = ISNULL(DatabaseTask.ArchiveStorageFileName,  DatabaseTask.RawStorageFileName) 

	,@BronzeWorkspaceName = DatabaseTask.SinkWorkspaceName
	,@BronzeLakehouseName = DatabaseTask.SinkLakehouseName
    ,@BronzeSchemaName = DatabaseTask.SinkSchemaName
    ,@BronzeTableName = DatabaseTask.SinkTableName
    ,@BronzeLoadMethod = DatabaseTask.SinkLoadMethod
    ,@BronzeWorkspaceId = DatabaseTask.SinkWorkspaceId
    ,@BronzeLakehouseId = DatabaseTask.SinkLakehouseId
    ,@WatermarkColumn = DatabaseTask.SinkWatermarkColumn
    ,@SinkTableName = DatabaseTask.SinkTableName
	,@SinkSchemaName = DatabaseTask.SinkSchemaName
	,@SinkWatermarkColumn = DatabaseTask.SinkWatermarkColumn
    ,@SinkLoadMethod = DatabaseTask.SinkLoadMethod

	,@IsWatermarkEnabledFlag = DatabaseTask.IsWatermarkEnabledFlag

FROM etl.Task as DatabaseTask
WHERE DatabaseTask.TaskKey = @TaskKey
;


BEGIN
	SET @LogMessage = 'From etl.GetDatabaseTaskInfo: SourceName = "' + ISNULL(@SourceName, '(NULL)') + '"'
	EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	SET @LogMessage = 'From etl.GetDatabaseTaskInfo: SourceType = "' + ISNULL(@sourceType, '(NULL)') + '"'
	EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	SET @LogMessage = 'From etl.GetDatabaseTaskInfo: SourceDatabaseName = "' + ISNULL(@SourceDatabaseName, '(NULL)') + '"'
	EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	SET @LogMessage = 'From etl.GetDatabaseTaskInfo: SourceSchemaName = "' + ISNULL(@SourceSchemaName, '(NULL') + '"'
	EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	SET @LogMessage = 'From etl.GetDatabaseTaskInfo: SourceTableName = "' + ISNULL(@SourceTableName, '(NULL)') + '"'
	EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	SET @LogMessage = 'From etl.GetDatabaseTaskInfo: PrimaryKeyColumnList = "' + ISNULL(@primaryKeyColumnList, '(NULL)') + '"'
	EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	SET @LogMessage = 'From etl.GetDatabaseTaskInfo: RawStoragePath = "' + ISNULL(@RawStoragePath, '(NULL)') + '"'
	EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	SET @LogMessage = 'From etl.GetDatabaseTaskInfo: RawStorageFileName = "' + ISNULL(@RawStorageFileName, '(NULL)') + '"'
	EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	SET @LogMessage = 'From etl.GetDatabaseTaskInfo: ArchiveOriginalFilesFlag = "' + ISNULL(CAST(@ArchiveOriginalFilesFlag AS VARCHAR(1)), '(NULL)') + '"'
	EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	SET @LogMessage = 'From etl.GetDatabaseTaskInfo: ArchiveStoragePath = "' + ISNULL(@ArchiveStoragePath, '(NULL)') + '"'
	EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	SET @LogMessage = 'From etl.GetDatabaseTaskInfo: ArchiveStorageFileName = "' + ISNULL(@ArchiveStorageFileName, '(NULL') + '"'
	EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	SET @LogMessage = 'From etl.GetDatabaseTaskInfo: BronzeWorkspaceName = "' + ISNULL(@BronzeWorkspaceName, '(NULL)') + '"'
    EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	SET @LogMessage = 'From etl.GetDatabaseTaskInfo: BronzeWorkspaceId = "' + ISNULL(@BronzeWorkspaceId, '(NULL)') + '"'
	EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	SET @LogMessage = 'From etl.GetDatabaseTaskInfo: BronzeLakehouseName = "' + ISNULL(@BronzeLakehouseName, '(NULL)') + '"'
    EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	SET @LogMessage = 'From etl.GetDatabaseTaskInfo: BronzeLakehouseId = "' + ISNULL(@BronzeLakehouseId, '(NULL)') + '"'
    EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	SET @LogMessage = 'From etl.GetDatabaseTaskInfo: WatermarkColumn = "' + ISNULL(@WatermarkColumn, '(NULL)') + '"'
	EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	SET @LogMessage = 'From etl.GetDatabaseTaskInfo: BronzeSchemaName = "' + ISNULL(@BronzeSchemaName, '(NULL)') + '"'
	EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	SET @LogMessage = 'From etl.GetDatabaseTaskInfo: BronzeTableName = "' + ISNULL(@BronzeTableName, '(NULL') + '"'
	EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	SET @LogMessage = 'From etl.GetDatabaseTaskInfo: BronzeLoadMethod = "' + ISNULL(@BronzeLoadMethod, '(NULL)') + '"'
	EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
	SET @LogMessage = 'From etl.GetDatabaseTaskInfo: IsWatermarkEnabledFlag = "' + ISNULL(CAST(@IsWatermarkEnabledFlag AS VARCHAR(1)), '(NULL)') + '"'
	EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
END

-- now return the metadata
SELECT
@JobAuditKey as JobAuditKey
,@TaskKey AS TaskKey
,@TaskType as TaskType
,@SourceName AS SourceName
,@SourceType AS SourceType

,@SourceDatabaseName AS SourceDatabaseName
,@SourceSchemaName AS SourceSchemaName
,@SourceTableName AS SourceTableName
,ISNULL(@PrimaryKeyColumnList, '') AS PrimaryKeyColumnList

,@RawStoragePath AS RawStoragePath
,@RawStorageFileName AS RawStorageFileName

,@ArchiveOriginalFilesFlag AS ArchiveOriginalFilesFlag
,@ArchiveStoragePath AS ArchiveStoragePath
,@ArchiveStorageFileName as ArchiveStorageFileName

,@BronzeWorkspaceName AS BronzeWorkspaceName
,@BronzeLakehouseName AS BronzeLakehouseName
,@BronzeSchemaName AS BronzeSchemaName
,@BronzeTableName AS BronzeTableName
,@BronzeLoadMethod AS BronzeLoadMethod
,@BronzeWorkspaceId AS BronzeWorkspaceId
,@BronzeLakehouseId AS BronzeLakehouseId
,@WatermarkColumn AS WatermarkColumn
,@SinkTableName AS SinkTableName
,@SinkSchemaName AS SinkSchemaName
,@SinkWatermarkColumn AS SinkWatermarkColumn
,@SinkLoadMethod AS SinkLoadMethod

,@IsWatermarkEnabledFlag AS IsWatermarkEnabledFlag
END
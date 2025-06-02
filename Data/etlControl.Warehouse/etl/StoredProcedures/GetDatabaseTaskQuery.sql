CREATE   PROCEDURE [etl].[GetDatabaseTaskQuery]
(@TaskKey INT, @TaskAuditKey BIGINT)
AS
BEGIN
/******************************************************************************
PURPOSE: This procedure constructs the query to run in the source database. 
Depending on the SourceExtractionMethod, the query generated will differ
-- exec [etl].[GetDatabaseTaskQuery] 76, 1599

******************************************************************************/

SET NOCOUNT ON

-- vars for utility
DECLARE @ErrorMessage NVARCHAR(2048) 
DECLARE @LogMessage NVARCHAR(4000)

-- validate TaskAuditKey and throw error if invalid as rest of proc will not work properly
IF NOT EXISTS (SELECT TOP 1 TaskKey FROM etl.TaskAudit WHERE TaskAuditKey = @TaskAuditKey AND TaskKey = @TaskKey)
BEGIN
	SET @errorMessage =  'Error: Lookup to etl.TaskAudit failed for TaskKey:  "' + ISNULL(CONVERT(VARCHAR(20), @TaskKey), 'NULL') + '" and TaskAuditKey: "' + ISNULL(CONVERT(VARCHAR(20), @TaskAuditKey), 'NULL') + '" Please verify the TaskKey and TaskAuditKey parameterS passed to proc etl.GetDatabaseTaskQuery.';
	THROW 50000, @errorMessage, 1;
END

-- Note: SourceType and Connection info already validated in GetDatabaseTaskInfo proc
-- dervied vars
DECLARE @Query NVARCHAR(MAX)

-- metadata from etl.DatabaseTask
DECLARE @SourceName NVARCHAR(200) 
DECLARE @SourceType NVARCHAR(200)
DECLARE @SourceDatabaseName NVARCHAR(200)
DECLARE @SourceSchemaName NVARCHAR(200) -- (possibly required)
DECLARE @SourceTableName NVARCHAR(200) -- (required unless "SourceQuery" is used)
DECLARE @SourceWhereClause NVARCHAR(MAX) -- (optional) used with SourceTableName to support custom filtering, also required for incremental functionality
DECLARE @SourceColumnList NVARCHAR(MAX) -- (optional) if left blank than all columns will be imported
DECLARE @PrimaryKeyColumnList NVARCHAR(2000) -- (optional but needed for incremental and merge functionality)
DECLARE @OverrideQuery NVARCHAR(MAX) -- (optional) a customized query that overrrides the SourceTableName 
DECLARE @IsWatermarkEnabledFlag BIT -- (optional) enables incremental-loading functionality
DECLARE @HighWatermarkValue NVARCHAR(200) -- (optional but required for incremental) 
DECLARE @WatermarkColumn NVARCHAR(200) -- (optional but required for incremental) 

DECLARE @BronzeWorkspaceName NVARCHAR(2000)
DECLARE @BronzeLakehouseName NVARCHAR(2000)
DECLARE @BronzeSchemaName NVARCHAR(2000)
DECLARE @BronzeTableName NVARCHAR(2000)
DECLARE @BronzeLoadMethod NVARCHAR(40)
DECLARE @SourceExtractionMethod NVARCHAR(40)

-- vars for building query
DECLARE @QuerySelectClause NVARCHAR(MAX)
DECLARE @QueryFromClause NVARCHAR(400)
DECLARE @QueryWhereClause NVARCHAR(MAX)
-- for handling brackets in schema and table names
DECLARE @SourceSchemaNameBrackets NVARCHAR(200)
DECLARE @SourceSchemaNameNoBrackets NVARCHAR(200)
DECLARE @SourceTableNameBrackets NVARCHAR(200)
DECLARE @SourceTableNameNoBrackets NVARCHAR(200)
DECLARE @SourceSchemaAndTableBrackets NVARCHAR(400)
DECLARE @SourceSchemaAndTableNoBrackets NVARCHAR(400)
DECLARE @PrimaryKeyColumnListNoBrackets NVARCHAR(2000)


SELECT
	@SourceName = DatabaseTask.SourceName, 
    @SourceType = LOWER(DatabaseTask.SourceType),
	@SourceDatabaseName = ISNULL(DatabaseTask.SourceDatabaseName, ''), 
    @SourceSchemaName = ISNULL(DatabaseTask.SourceSchemaName, ''), 
    @SourceTableName = ISNULL(DatabaseTask.SourceTableName, ''),
    @PrimaryKeyColumnList = ISNULL(DatabaseTask.PrimaryKeyColumnList, ''),
    
    @IsWatermarkEnabledFlag = ISNULL(DatabaseTask.IsWatermarkEnabledFlag, 0),
    @WatermarkColumn = ISNULL(DatabaseTask.SinkWatermarkColumn, ''),
	@SourceWhereClause = ISNULL(DatabaseTask.SourceWhereClause, ''),
    @HighWatermarkValue = ISNULL(Watermark.HighWatermarkValue, ''),

	@BronzeWorkspaceName = DatabaseTask.SinkWorkspaceName,
	@BronzeLakehouseName = DatabaseTask.SinkLakehouseName,
	@BronzeSchemaName = DatabaseTask.SinkSchemaName,
	@BronzeTableName = DatabaseTask.SinkTableName,

    @BronzeLoadMethod = LOWER(DatabaseTask.SinkLoadMethod),
    @SourceExtractionMethod = LOWER(DatabaseTask.SourceExtractionMethod),
    @OverrideQuery = DatabaseTask.OverrideQuery

FROM etl.Task AS DatabaseTask
LEFT JOIN etl.Watermark
ON DatabaseTask.TaskKey = Watermark.TaskKey
WHERE DatabaseTask.TaskKey = @TaskKey

-- some databases surround identifiers with square brackets
-- add/remove brackets to have for various usage
IF @SourceSchemaName IS NOT NULL OR @SourceSchemaName != ''
    BEGIN
        IF CHARINDEX('[',@SourceSchemaName) = 0
            BEGIN
                SET @SourceSchemaNameBrackets = '[' + @SourceSchemaName + ']'
                SET @SourceSchemaNameNoBrackets = @SourceSchemaName
            END
        ELSE
            BEGIN
                SET @SourceSchemaNameBrackets = @SourceSchemaName
                SET @SourceSchemaNameNoBrackets = REPLACE(REPLACE(@SourceSchemaName,'[',''),']','')
            END
    END
ELSE --  no schema provided, so set to empty string so null won't affect string concats
    BEGIN
        SET @SourceSchemaNameBrackets = ''
        SET @SourceSchemaNameNoBrackets = ''
    END

IF CHARINDEX('[',@SourceTableName) = 0
    BEGIN
        SET @SourceTableNameBrackets = '[' + @SourceTableName + ']'
        SET @SourceTableNameNoBrackets = @SourceTableName
    END
ELSE
    BEGIN
        SET @SourceTableNameBrackets = @SourceTableName
        SET @SourceTableNameNoBrackets = REPLACE(REPLACE(@SourceTableName,'[',''),']','') ;
    END

SET @SourceSchemaAndTableBrackets = CASE WHEN @SourceSchemaNameBrackets IS NULL OR @SourceSchemaNameBrackets = '' THEN @SourceTableNameBrackets
                                        ELSE @SourceSchemaNameBrackets + '.' + @SourceTableNameBrackets
                                        END

-- if override query, then skip building the query but still perform any token substitutions
IF @SourceExtractionMethod = 'override-query'
    BEGIN
        IF @OverrideQuery IS NULL
            BEGIN
                SET @errorMessage =  'Error: OverrideQuery is missing (NULL) TaskKey:  "' + ISNULL(CONVERT(VARCHAR(20), @TaskKey), 'NULL')  + '" SourceExtractionMethod = query which requires a value for etl.DatabaseTask.OverrideQuery. Or change the SourceExtractionMethod.';
                THROW 50000, @errorMessage, 1;
            END
    
         SET @Query = @OverrideQuery
    END  -- IF lower(@SourceExtrationMethod) = 'override-query'

IF @SourceExtractionMethod IN ('full-query','filter-query','incremental-query')
    BEGIN
		-- these methods require building a query so first create the SELECT, FROM, WHERE clauses then concat at end

		-- SELECT CLAUSE
        SET @QuerySelectClause = 'SELECT '
                    
        SET @QuerySelectClause = @QuerySelectClause + '*'

        -- FROM CLAUSE

        SET @QueryFromClause = ' FROM '

		IF @SourceType IN ('sqlserver')
            BEGIN
                IF @SourceSchemaName != ''
					SET @QueryFromClause = @QueryFromClause + UPPER(@SourceSchemaName) + '.'

				SET @QueryFromClause = @QueryFromClause + UPPER(@SourceTableName) + ' WITH (NOLOCK) '

				-- update hwm value
				-- expected format: TO_TIMESTAMP_TZ('2024-01-25 12:34:56.789 +0000') 
                SET @HighWatermarkValue = 'TO_TIMESTAMP_TZ(''' + @HighWatermarkValue + ' +0000'')'
            END


		IF @SourceType IN ('snowflake')
			BEGIN
				IF @SourceSchemaName != ''
					SET @QueryFromClause = @QueryFromClause + UPPER(@SourceSchemaName) + '.'

				SET @QueryFromClause = @QueryFromClause + UPPER(@SourceTableName) + ' '

				-- update hwm value
				-- expected format: TO_TIMESTAMP_TZ('2024-01-25 12:34:56.789 +0000') 
                SET @HighWatermarkValue = 'TO_TIMESTAMP_TZ(''' + @HighWatermarkValue + ' +0000'')'
			END
        -- databases that surround identifiers with double quotes
        IF @SourceType IN ('oracle') 
			BEGIN
				IF @SourceSchemaName != ''
					IF CHARINDEX('"', @SourceSchemaName) > 0
						SET @QueryFromClause = @QueryFromClause + @SourceSchemaName + '.' -- if the schema name includes double quotes already, then don't add them, and don't automatically convert to uppercase
					ELSE
						SET @QueryFromClause = @QueryFromClause + '"' + UPPER(@SourceSchemaName) + '".'

				IF CHARINDEX('"', @SourceTableName) > 0
					SET @QueryFromClause = @QueryFromClause + @SourceTableName + ' ' -- if the table name includes double quotes already, then don't add them, and don't automatically convert to uppercase
				ELSE
					SET @QueryFromClause = @QueryFromClause + '"' + UPPER(@SourceTableName) + '" '

				-- update hwm value
                SET @HighWatermarkValue = 'TO_DATE(''' + @HighWatermarkValue + ''', ''YYYY-MM-DD HH24:MI:SS'')'
			END


        -- WHERE CLAUSE
        IF @SourceWhereClause = ''
			BEGIN
				IF @IsWatermarkEnabledFlag = 1
					SET @QueryWhereClause = 'WHERE ' + @WatermarkColumn + ' > ' + @HighWatermarkValue
				ELSE 
					SET @QueryWhereClause = @SourceWhereClause
			END

        ELSE -- where clause provided
			BEGIN
				-- if clause is missing the keyword WHERE, then add it
				IF UPPER(@SourceWhereClause) NOT LIKE 'WHERE %'
					SET @QueryWhereClause = 'WHERE ' + @SourceWhereClause
				ELSE
					SET @QueryWhereClause = @SourceWhereClause
				IF @IsWatermarkEnabledFlag = 1
					SET @QueryWhereClause = @QueryWhereClause + ' AND ' + @WatermarkColumn + ' > ' + @HighWatermarkValue
			END
        -- now concat the clauses to form complete query
        SET @Query = @QuerySelectClause + @QueryFromClause + ISNULL(@QueryWhereClause,'')
    END

-- Final steps - query should be created and any tokens replaced so validate, log, and return query
-- validate query, sometimes nulls can cause problems when concatenating strings
IF @Query IS NULL or @Query = ''
	BEGIN
		SET @errorMessage =  'Error: Query is empty for DatabaseTask:  "' + ISNULL(CONVERT(VARCHAR(20), @TaskKey), 'NULL') + '" and TaskAuditKey: "' + ISNULL(CONVERT(VARCHAR(20), @TaskAuditKey), 'NULL') + '" Please verify the settings in DatabaseTask.';
		THROW 50000, @errorMessage, 1;
	END

-- determine if debug mode is turned one
--DECLARE @DebugMode INT = 0
--SELECT @DebugMode = CAST( ConfigValue AS INT) FROM etl.GlobalConfig WHERE ConfigKey = 'DebugMode'

--IF @DebugMode = 1 
  BEGIN
    SET @LogMessage = 'Using query: ' + ISNULL(@Query,'NULL')
    EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage
  END

SELECT @Query AS SourceQuery, @IsWatermarkEnabledFlag as IsWatermarkEnabledFlag
END
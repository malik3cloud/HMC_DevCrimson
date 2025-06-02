CREATE     PROCEDURE [etl].[TaskAuditExecutionUpdate] 
(
	@TaskAuditKey BIGINT, 
	@RowsRead BIGINT, 
	@RowsCopied BIGINT, 
	@RowsSkipped BIGINT, 
	@RowsInserted BIGINT, 
	@RowsUpdated BIGINT, 
	@RowsDeleted BIGINT, 
	@IsPartitionFlag BIT
)
AS
BEGIN
/******************************************************************************
PURPOSE: This procedure updates etl.TaskAudit with metadata for data metrics
If IsPartitionFlag is true, then it will add to any existing metrics to keep
a running total until all partitions are processed and the final metrics
will reflect the totals.

******************************************************************************/
SET NOCOUNT ON;

DECLARE @LogMessage NVARCHAR(4000);
DECLARE @ApiAuditKey INT;
DECLARE @CurrentUTCDateTime DATETIME 
SET @CurrentUTCDateTime = GETUTCDATE()

SET @LogMessage = 'Updating TaskAudit with data load metrics.'
EXEC etl.LogMessage @JobAuditKey = NULL, @TaskAuditKey = @TaskAuditKey, @Message = @LogMessage

-- check if partition loading to keep accumulating metrics for totals if true
-- partition means a single table will have multipe copy activities per partition
-- so we want to keep a running total of the data metrics for each partition until entire table is loaded
-- and TaskAudit will reflect the total metrics for the table, not just a single partition

IF ISNULL(@IsPartitionFlag, 0) = 0
	BEGIN
	UPDATE etl.TaskAudit
	SET 
		RowsRead = ISNULL(@RowsRead, RowsRead),
		RowsCopied = ISNULL(@RowsCopied, RowsCopied),
		RowsSkipped = ISNULL(@RowsSkipped, RowsSkipped),
		RowsInserted = ISNULL(@RowsInserted, RowsInserted),
		RowsUpdated = ISNULL(@RowsUpdated, RowsUpdated),
		RowsDeleted = ISNULL(@RowsDeleted, RowsDeleted),
		LastUpdated = @CurrentUTCDateTime
	WHERE TaskAuditKey = @TaskAuditKey
	END
ELSE
	BEGIN
	UPDATE etl.TaskAudit
	SET 
		RowsRead = (ISNULL(RowsRead,0) + ISNULL(@RowsRead,0)),
		RowsCopied =  (ISNULL(RowsCopied,0) + ISNULL(@RowsCopied,0)),
		LastUpdated = @CurrentUTCDateTime
	WHERE TaskAuditKey = @TaskAuditKey
	END

END
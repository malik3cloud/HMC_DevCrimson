CREATE TABLE [etl].[TaskAudit] (

	[TaskAuditKey] bigint NOT NULL, 
	[JobAuditKey] bigint NULL, 
	[TaskKey] int NULL, 
	[TaskType] varchar(100) NULL, 
	[TaskRunOrderNbr] int NOT NULL, 
	[Status] varchar(400) NULL, 
	[IsRunning] bit NOT NULL, 
	[StartTime] datetime2(6) NULL, 
	[EndTime] datetime2(6) NULL, 
	[RowsRead] bigint NULL, 
	[RowsCopied] bigint NULL, 
	[RowsSkipped] bigint NULL, 
	[RowsInserted] bigint NULL, 
	[RowsUpdated] bigint NULL, 
	[RowsDeleted] bigint NULL, 
	[TaskVariables] varchar(4000) NULL, 
	[DebugString] varchar(4000) NULL, 
	[PipelineRunID] varchar(100) NULL, 
	[LastUpdated] datetime2(6) NULL
);
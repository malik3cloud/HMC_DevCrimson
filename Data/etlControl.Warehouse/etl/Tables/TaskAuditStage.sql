CREATE TABLE [etl].[TaskAuditStage] (

	[TaskAuditLogKey] bigint NOT NULL, 
	[TaskAuditKey] bigint NOT NULL, 
	[TaskKey] bigint NOT NULL, 
	[RequestedStatus] varchar(50) NOT NULL, 
	[RequestedStartTime] datetime2(6) NULL, 
	[Processed] bit NULL, 
	[ProcessedTime] datetime2(6) NULL, 
	[CompanyId] bigint NULL, 
	[LogMessage] varchar(4000) NULL, 
	[RowsRead] bigint NULL, 
	[RowsUpdated] bigint NULL, 
	[RowsInserted] bigint NULL, 
	[RowsDeleted] bigint NULL, 
	[RequestedEndTime] datetime2(6) NULL
);
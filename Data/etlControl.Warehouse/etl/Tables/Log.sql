CREATE TABLE [etl].[Log] (

	[LogKey] bigint NOT NULL, 
	[JobAuditKey] bigint NULL, 
	[TaskAuditKey] bigint NULL, 
	[PipelineRunID] varchar(100) NULL, 
	[LogDatetime] datetime2(6) NOT NULL, 
	[Message] varchar(4000) NULL
);
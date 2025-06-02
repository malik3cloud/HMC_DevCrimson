CREATE TABLE [etl].[JobAudit] (

	[JobAuditKey] bigint NOT NULL, 
	[JobKey] int NULL, 
	[JobName] varchar(200) NULL, 
	[IsAdHocJob] bit NOT NULL, 
	[StartTime] datetime2(6) NULL, 
	[RestartTime] datetime2(6) NULL, 
	[EndTime] datetime2(6) NULL, 
	[Status] varchar(50) NULL, 
	[FabricWorkspaceID] varchar(200) NULL, 
	[PipelineRunID] varchar(100) NULL, 
	[LastUpdated] datetime2(6) NULL
);
CREATE TABLE [etl].[Watermark] (

	[TaskKey] int NULL, 
	[FileTaskKey] int NULL, 
	[HighWatermarkValue] varchar(200) NULL
);
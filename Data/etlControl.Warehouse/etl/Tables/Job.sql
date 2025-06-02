CREATE TABLE [etl].[Job] (

	[JobKey] int NOT NULL, 
	[JobName] varchar(500) NOT NULL, 
	[JobDescription] varchar(500) NULL, 
	[IsActiveFlag] bit NOT NULL
);
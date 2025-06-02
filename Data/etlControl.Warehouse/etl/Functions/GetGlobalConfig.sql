CREATE   FUNCTION [etl].[GetGlobalConfig]
(
    @ConfigKey NVARCHAR(200)
)
RETURNS TABLE
AS
RETURN (


    SELECT TOP 1
        ConfigValue 
    FROM etl.GlobalConfig 
    WHERE ConfigKey = @ConfigKey 

)
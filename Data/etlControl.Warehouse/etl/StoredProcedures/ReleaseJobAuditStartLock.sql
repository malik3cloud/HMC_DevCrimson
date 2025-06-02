CREATE PROCEDURE etl.ReleaseJobAuditStartLock
AS
/******************************************************************************
PURPOSE: This procedure makes sure that SPJobAuditStartLock is always 0
when we start the master pipeline
******************************************************************************/
IF NOT EXISTS (SELECT 1 FROM etl.GlobalConfig WHERE ConfigKey = 'SPJobAuditStartLock')
    BEGIN
        INSERT INTO etl.GlobalConfig (ConfigKey, ConfigValue)
        VALUES ('SPJobAuditStartLock', '0')
    END
ELSE
    BEGIN
        UPDATE etl.GlobalConfig
        SET ConfigValue = '0'
        WHERE ConfigKey = 'SPJobAuditStartLock'
    END
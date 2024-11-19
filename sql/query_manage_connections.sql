USE master;
GO
SELECT 
    session_id, 
    host_name, 
    program_name, 
    login_name, 
    status 
FROM 
    sys.dm_exec_sessions
WHERE 
    database_id = DB_ID( 'DRE_Cleaned_Data' );

kill 101;

DROP DATABASE [DRE_Cleaned]
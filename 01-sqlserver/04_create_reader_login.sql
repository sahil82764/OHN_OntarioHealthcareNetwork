/* A dedicated read-only login for the Fabric gateway connection.

   Pointing a gateway at sa works and is exactly the habit that turns a
   portfolio project into a security finding. This login can read the three
   source databases and nothing else. */

USE master;
GO
IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = 'fabric_reader')
    CREATE LOGIN fabric_reader WITH PASSWORD = '$(ReaderPassword)',
        CHECK_POLICY = ON;
ELSE
    ALTER LOGIN fabric_reader WITH PASSWORD = '$(ReaderPassword)';
GO

DECLARE @db SYSNAME, @sql NVARCHAR(MAX);
DECLARE db_cur CURSOR FOR
    SELECT name FROM sys.databases WHERE name IN ('OHN_EHR','OHN_SCHED','OHN_FIN');
OPEN db_cur;
FETCH NEXT FROM db_cur INTO @db;
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql = N'USE ' + QUOTENAME(@db) + N';
        IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = ''fabric_reader'')
            CREATE USER fabric_reader FOR LOGIN fabric_reader;
        ALTER ROLE db_datareader ADD MEMBER fabric_reader;';
    EXEC sp_executesql @sql;
    FETCH NEXT FROM db_cur INTO @db;
END
CLOSE db_cur;
DEALLOCATE db_cur;
GO

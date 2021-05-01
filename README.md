# Snowflake Stored Procedure migration
## Architecture
This framework consists of 5 components:

Component  | Component name | Description
------------- | ------------- | -------------
ProcTable  | GetTest1 | One ProcTable per stored procedure that stores every line of code as a row
ProcTableGenerator  | GenerateProcTable MultipleInserts.html | Helps create ProcTable DDL and convert Snowflake code to ProcTable inserts.
Audit Table  |SP_AUDITLOG |  Logs a record for each sql , its execution time,  status, rows affected
Proc Excution Table  |SP_PROC_EXECUTION |  Stores the list of ProcTables in order of dependencies. This table is iterated from ETL tool.
Target Table  |Test1 |  Final Dimension or Fact table.

## Components
##### ProcTable [GetTest1]
![folder](/ScreenShots/2_ProcTable_DEMO_DB.RPO.GETTEST1.png?raw=true)

##### ProcTableGenerator [GenerateProcTableMultipleInserts.html]
![folder](/ScreenShots/1_Snowflake ProcTable Generator.png?raw=true)

##### Audit Table [SP_AUDITLOG]
![folder](/ScreenShots/3_AuditTable.png?raw=true)

##### Proc Excution Table [SP_PROC_EXECUTION]
![folder](/ScreenShots/4_sp_proc-execution.png?raw=true)

## Steps:
##### Script1.sql
1. Create repository schema [DEMO_DB.RPO]
2. Create table DEMO_DB.RPO.SP_AUDITLOG
3. Create table DEMO_DB.RPO.SP_PROC_EXECUTION
4. Create Stored Procedure EXEC_PROC_FROM_JOB

##### Script2.sql
1. Create Test1 base table
2. Test if the insert statements have correct syntax
3. Use GenerateProcTableMultipleInserts.html to create your ProcTable DDL and inserts.
4. Execute the ProcTable DDL and inserts to Snowflake and check the data
5. Execute stored Procedure EXEC_PROC_FROM_JOB('RPO.GETTEST1', 1);
6. Check audit table and target table
7. Insert to SP_PROC_EXECUTION [to be run from ETL tool/scheduler]

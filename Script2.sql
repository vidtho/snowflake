-- Proc Table RPO.GETTEST1

-- Step 1. Create Test1 base table
create or replace table DEMO_DB.RPO.TEST1 (
  AAAA  int, 
  BBBB  varchar, 
  CCCC  varchar);

-- Step 2. Test if the insert statements have correct syntax
truncate table RPO.TEST1

INSERT INTO RPO.TEST1 (AAAA, BBBB, CCCC)  
SELECT 1, 
  'Test1' AS bb,
  'Test1' AS cc
FROM dual

INSERT INTO RPO.TEST1 (AAAA, BBBB, CCCC)  
VALUES(2, 'Test2', 'Test2')

UPDATE RPO.TEST1 SET CCCC = 'Description for '||CCCC;

select * from RPO.TEST1
--==================================================
-- Step 3. use GenerateProcTableMultipleInserts.html to create your Proc Table
--         copy the SQL SCripts Results and execute in snowflake

use database DEMO_DB;

create or replace TABLE RPO.GETTEST1
 (
    SQL_POSITION INT,
    SQL_DESC VARCHAR,
    SQL_VARCHAR VARCHAR, -- will be used for testing the stmt
    SQL_VARIANT VARIANT,  -- will be used by the procedure
    ACTIVE_FLAG INT DEFAULT 1
);

Truncate TABLE RPO.GETTEST1;

INSERT INTO RPO.GETTEST1(SQL_POSITION, SQL_DESC, SQL_VARCHAR, SQL_VARIANT)
SELECT 10, 'truncate table RPO.TEST1',
'truncate table RPO.TEST1',
TO_VARIANT('truncate table RPO.TEST1') 
FROM DUAL;

INSERT INTO RPO.GETTEST1(SQL_POSITION, SQL_DESC, SQL_VARCHAR, SQL_VARIANT)
SELECT 11, 'INSERT INTO RPO.TEST1 (AAAA, BBBB,',
'INSERT INTO RPO.TEST1 (AAAA, BBBB, CCCC)  
SELECT 1, 
  ''Test1'' AS bb,
  ''Test1'' AS cc
FROM dual',
TO_VARIANT('INSERT INTO RPO.TEST1 (AAAA, BBBB, CCCC)  
SELECT 1, 
  ''Test1'' AS bb,
  ''Test1'' AS cc
FROM dual') 
FROM DUAL;

INSERT INTO RPO.GETTEST1(SQL_POSITION, SQL_DESC, SQL_VARCHAR, SQL_VARIANT)
SELECT 12, 'INSERT INTO RPO.TEST1 (AAAA, BBBB,',
'INSERT INTO RPO.TEST1 (AAAA, BBBB, CCCC)  
VALUES(2, ''Test2'', ''Test2'')',
TO_VARIANT('INSERT INTO RPO.TEST1 (AAAA, BBBB, CCCC)  
VALUES(2, ''Test2'', ''Test2'')') 
FROM DUAL;

INSERT INTO RPO.GETTEST1(SQL_POSITION, SQL_DESC, SQL_VARCHAR, SQL_VARIANT)
SELECT 13, 'UPDATE RPO.TEST1 SET CCCC =',
'UPDATE RPO.TEST1 SET CCCC = ''Description for ''||CCCC;',
TO_VARIANT('UPDATE RPO.TEST1 SET CCCC = ''Description for ''||CCCC;') 
FROM DUAL;

--==================================================
-- Step 4. Check your proc Table 
--         copy the SQL SCripts Results and execute in snowflake
select * from DEMO_DB.RPO.GETTEST1 order by sql_position

-- Step 5: Execute Stored Proc
select * from DEMO_DB.RPO.TEST1
truncate table DEMO_DB.RPO.TEST1 
CALL DEMO_DB.RPO.EXEC_PROC_FROM_JOB('RPO.GETTEST1', 1);

Output:
{
  "Jobstatus": "[0] Success",
  "ret": "10, truncate table RPO.TEST1| 11, INSERT INTO RPO.TEST1 (AAAA, BBBB,| 12, INSERT INTO RPO.TEST1 (AAAA, BBBB,| 13, UPDATE RPO.TEST1 SET CCCC =| ",
  "stackmsg": [
    {
      "prc": "EXECPROC checkpoint 1:2021-05-01T17:35:18.799Z",
      "txt": "Begin Loop"
    },
    {
      "prc": "EXECPROC checkpoint 2:2021-05-01T17:35:18.800Z",
      "txt": "truncate table RPO.TEST1"
    },
    {
      "prc": "EXECPROC checkpoint 2:2021-05-01T17:35:20.477Z",
      "txt": "INSERT INTO RPO.TEST1 (AAAA, BBBB,"
    },
    {
      "prc": "EXECPROC checkpoint 2:2021-05-01T17:35:22.137Z",
      "txt": "INSERT INTO RPO.TEST1 (AAAA, BBBB,"
    },
    {
      "prc": "EXECPROC checkpoint 2:2021-05-01T17:35:24.008Z",
      "txt": "UPDATE RPO.TEST1 SET CCCC ="
    },
    {
      "prc": "EXECPROC checkpoint 3:2021-05-01T17:35:25.851Z",
      "txt": "End loop"
    }
  ]
}

-- Step 6: check audit table and target table
select * from DEMO_DB.RPO.SP_AUDITLOG;
select * from DEMO_DB.RPO.TEST1;

-- step 7: insert to SP_PROC_EXECUTION [to be run from scheduler]
INSERT INTO DEMO_DB.RPO.SP_PROC_EXECUTION (PROC_SEQ, PROC_NAME, TARGET_TABLE, ACTIVE_FLAG, DEPENDENCY_LEVEL, DEPENDENCY_TABLES) 
Values (1, 'RPO.GETTEST1', 'RPO.TEST1', 1, 1, ' ');

select * from  DEMO_DB.RPO.SP_PROC_EXECUTION
CREATE SCHEMA DEMO_DB.RPO;

-- Create table SP_AUDITLOGSEQ
--==============================
-- this will be used to log each sql statements in Proc Table, their timings, status and rows affected

CREATE SEQUENCE IF NOT EXISTS DEMO_DB.RPO.SP_AUDITLOGSEQ
    START 1
    INCREMENT 1;

CREATE OR REPLACE TABLE DEMO_DB.RPO.SP_AUDITLOG
(
  AUDIT_LOG_ID INT DEFAULT DEMO_DB.RPO.SP_AUDITLOGSEQ.NEXTVAL,
  JOB_NAME      VARCHAR,
  PROC_TABLE    VARCHAR,
  RUN_DATE      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  STATUS        VARCHAR,
  ROWCOUNT      INT,
  MESSAGE       VARCHAR,
  JOB_ID        VARCHAR
);


-- Create table SP_PROC_EXECUTION
--==============================
-- this will be called from etl tool iteratively order by PROC_SEQ
-- currently the table need to be added in order of dependency

CREATE OR REPLACE TABLE DEMO_DB.RPO.SP_PROC_EXECUTION
(
  PROC_SEQ           INT ,
  PROC_NAME          VARCHAR,
  TARGET_TABLE       VARCHAR,
  ACTIVE_FLAG        INT DEFAULT 1,
  DEPENDENCY_LEVEL   INT, 
  DEPENDENCY_TABLES  VARCHAR,
  LAST_RUN_DATE      DATETIME
);


-- Create Stored Procedure EXEC_PROC_FROM_JOB
--========================================
-- this procedure will loop through the Proc Table pass as parameter
-- a. execute each row of sql statement
-- b. log each row of sql stmt to RPO.SP_AUDITLOG
-- c. retuns [0] Success or [-1] Fail along with timings and error message

create or replace procedure DEMO_DB.RPO.EXEC_PROC_FROM_JOB(PV_PROCTABLE VARCHAR2, PV_JOBID VARCHAR2)
    returns VARIANT not null
    language javascript
    EXECUTE AS CALLER
as
$$
    try {
        var rowcount = 0;
        var sqlpos = 0;
        var sqldesc = "";
        var return_value = "";

        var cusr_sqlstmt = "select * from " + PV_PROCTABLE +" where active_flag = 1 order by SQL_POSITION" ;
        var cusr_stmt = snowflake.createStatement({ sqlText: cusr_sqlstmt});
        var cusr = cusr_stmt.execute();
        var msg = [];

    msg.push({prc:'EXECPROC checkpoint 1:'+ new Date().toISOString(),txt:'Begin Loop'});

    /* Loop through cursor */    
    while (cusr.next())  {
        
        sqlpos = cusr.getColumnValue(1);
        sqldesc = cusr.getColumnValue(2);
        var sqlCmd1 = cusr.getColumnValue(3);
        msg.push({prc:'EXECPROC checkpoint 2:'+ new Date().toISOString(),txt:sqldesc});
        
        
        sqlStmt1 = snowflake.createStatement( {sqlText: sqlCmd1} );
        recordset1 = sqlStmt1.execute();
   
        sqlStmt2 = snowflake.createStatement( { sqlText: "SELECT ROWS_PRODUCED FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(RESULT_LIMIT=>100)) WHERE QUERY_ID = LAST_QUERY_ID()" } )
        recordset2 = sqlStmt2.execute();
        recordset2.next();
        rowcount = recordset2.getColumnValue(1);
        result = "Rows Affected: " + recordset2.getColumnValue(1);

        sqlCmd3 = "INSERT INTO RPO.SP_AUDITLOG (JOB_NAME, PROC_TABLE, STATUS, ROWCOUNT, MESSAGE, JOB_ID) VALUES ('StoredProc RPO.EXECPROCFROMJOB','"+PV_PROCTABLE+"', 'Success', '"+rowcount+"','"+sqldesc+"','"+PV_JOBID+"' )";
        sqlStmt3 = snowflake.createStatement({sqlText: sqlCmd3});
        recordset3 = sqlStmt3.execute(); 


        return_value += cusr.getColumnValue(1);
        return_value += ", " + cusr.getColumnValue(2);
        return_value += "| ";

        }
   msg.push({prc:'EXECPROC checkpoint 3:'+ new Date().toISOString(),txt:'End loop'});
   return {Jobstatus:"[0] Success", ret:return_value, stackmsg:msg};
     }
  catch (err) 
    {
    return_value =  "Failed: Code: | SQLPos: " + sqlpos + ",SQLDesc: "+ sqldesc + ",ErrCode: " + err.code + " | State: " + err.state;
    return_value += "| Message: " + err.message;
    return_value += "| Stack Trace:" + err.stackTraceTxt ;
    //return_value2 = return_value.replace(/\'/gi,'') //Remove Quotes
    return_value2 = return_value.replace(/\'/gi,'\'\'') //Add two Quotes to escape quotes in snowflake
    rowcount = -1;

    sqlCmd4 = "INSERT INTO RPO.SP_AUDITLOG (JOB_NAME, PROC_TABLE, STATUS, ROWCOUNT, MESSAGE) VALUES ('StoredProc RPO.EXECPROCFROMJOB','"+PV_PROCTABLE+"', 'Fail', '"+rowcount+"','"+return_value2+"','"+PV_JOBID+"' )";
    sqlStmt4 = snowflake.createStatement({sqlText: sqlCmd4});
    recordset4 = sqlStmt4.execute(); 

    return {Jobstatus:"[-1] Fail", ret:return_value, stackmsg:msg};
    }
$$;
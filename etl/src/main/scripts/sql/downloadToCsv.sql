set echo off;
set feedback off;
set linesize %%LINE_SIZE%%;
set pagesize 0;
set sqlprompt '';
set trimspool on;
set headsep off;
set termout off;

spool %%OUTPUT_FILE%%;

%%SQL_QUERY%%
%%SQL_CONDITION%%;

spool off;

exit;

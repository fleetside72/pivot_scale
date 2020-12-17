# execure the sql for scale which builds the sql and inserts into a table
$PGD -f gen_clone.sql
# pull the sql out of the table and write it to route directory
$PGD -c "SELECT t FROM fc.sql WHERE cmd = 'clone'" -t -A -o clone.sql

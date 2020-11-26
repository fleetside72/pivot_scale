# execure the sql for baseline which builds the sql and inserts into a table
$PGD -f generate_sql/gen_baseline.sql
# pull the sql out of the table and write it to route directory
$PGD -c "SELECT t FROM fc.sql WHERE cmd = 'baseline'" -t -A -o route_sql/baseline.sql

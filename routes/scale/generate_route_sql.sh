# execure the sql for scale which builds the sql and inserts into a table
$PGD -f routes/scale/gen_scale.sql
# pull the sql out of the table and write it to route directory
$PGD -c "SELECT t FROM fc.sql WHERE cmd = 'scale'" -t -A -o routes/scale/scale_final.sql

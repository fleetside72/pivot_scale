$PGD -f generate_sql/gen_baseline.sql
$PGD -c "SELECT t FROM fc.sql WHERE cmd = 'baseline'" -t -A -o route_sql/baseline.sql

required columns in target data:
* value
* cost
* units
* order_date 
* ship_date
* order_status  (default to 'CLOSED')
* version       (defatult to 'ACTUALS')
* iteration     (default to 'ACTUALS')
* logid         (default to null)
* tag           (default to null)
* comment       (default to null)

setup function

0. run `schema.sql` to create the application tables
1. run `target_info` to get the column info for the target sales table to `track`
2. manually map the columns to the app entities and parameters
3. run `clone_target.sql` to clone the target data and create any app columns not designated for a `clone` of the tracking table
4. run `clone_meta.sql` to copy the tracker table meta for the clone
5. run `build_master_tables.sql` to build the functional tables (ex. customer master)
6. run `perd.sql` to buil a static hard-coded notion of how periods are defined

| tname   | cname          | opos | func         | fkey         | pretty | dtype   | mastcol        | appcol       | dateref |
| ------- | -------------- | ---- | ------------ | ------------ | ------ | ------- | -------------- | ------------ | ------- |
| fc.live | fb_cst_loc     | 91   | cost         |              |        | numeric | fb_cst_loc     |              |         |
| fc.live | ship_cust      | 36   | scust        | scust        |        | text    | ship_cust      |              |         |
| fc.live | rdate          | 98   | rdate        | rdate        |        | date    | drange         |              |         |
| fc.live | geo            | 42   | scust        |              |        | text    | geo            | customer     |         |
| fc.live | part           | 54   | item         | item         |        | text    | part           | item         |         |
| fc.live | odate          | 96   | odate        | odate        |        | date    | drange         | order_date   |         |
| fc.live | sdate          | 100  | sdate        | sdate        |        | date    | sdate          | ship_date    |         |
| fc.live | oseas          | 97   | odate        |              |        | integer | ssyr           |              | ssyr    |
| fc.live | calc_status    | 94   | order_status | order_status |        | text    | calc_status    | order_status |         |
| fc.live | rseas          | 99   | rdate        |              |        | integer | ssyr           |              | ssyr    |
| fc.live | sseas          | 101  | sdate        |              |        | integer | ssyr           |              | ssyr    |


* func: table name of associated data
* fkey: primary key of assoicated dat
* pretty: display column name 
* mastcol: associated table column reference (whats the point of this?)
* appcol: parameters that will have to be supplied but the application
* dateref:

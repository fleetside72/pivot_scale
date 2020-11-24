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

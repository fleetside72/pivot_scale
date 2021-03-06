## worked on so far

setup
----------------------------------------------------------------------------------------------------------------------------------------------------
the basic assumption is a single sales table is available to work with that has a lot of related data that came from master data tables originally.
the goal then is to break that back apart to whatever degree is necessary.

* _**run**_ `schema.sql` and `perd.sql` to setup basic tables
* create a table fc.live as copied from target (will need to have columns `version` and `iter` added if not existing)
* _**run**_ `target_info.sql` to populate the `fc.target_meta` table that holds all the columns and their roles
* fill in flags on table `fc.target_meta` to show how the data is related
* _**run**_ `build_master_tables.sql` to generate foreign key based master data


routes
----------------------------------------------------------------------------------------------------------------------------------------------------
* all routes would be tied to an underlying sql that builds the incremental rows
* that piece of sql will have to be build based on the particular sales layout
    * **columns:** a function to build the columns for each route
    * **where** a function to build the where clause will be required for each route
    * the result of above will get piped into a master function that build the final sql
    * the master function will need to be called to build the sql statements into files of the project


route baseline
----------------------------------------------------------------------------------------------------------------------------------------------------
* forecast = baseline (copied verbatim from actuals and increment the dates) + diffs. if orders are canceled this will show up as differ to baseline
* regular updates to baseline may be required to keep up with canceled/altered orders
* copy some period of actual sales and increment all the dates to serve as a baseline forecast

- [x] join to period tables to populate season; requires variance number oof table joins, based on howmany date functions there are 🙄
- [ ] some of the app parameters can be consolidated, the baseline period could be one large range potentially, instead of 2 stacked periods
- [x] setup something to fill in sql parameters to do testing on the function
- [ ] update node to handle forecast name parameter
- [ ] calc status is hard-coded right now in the json request -> probably needs to be manuall supplied up front

scale
----------------------------------------------------------------------------------------------------------------------------------------------------
- [x] need to add version columns to all CTE's
- [ ] need to build log insert
- [x] Need to build where clause for scenario

running problem list
----------------------------------------------------------------------------------------------------------------------------------------------------
* baseline route
    - [x] problem: how will the incremented order season get updated, adding an interval won't work
        * a table fc.odate, has been built, but it is incomplete, a setup function filling in these date-keyed tables could be setup
        * if a table is date-keyed, fc.perd could be targeted to fill in the gaps by mapping the associated column names
    - [x] problem: the target sales data has to map have concepts like order_date, and the application needs to know which col is order date
        * add column called application hook
    - [ ] there is not currently any initial grouping to limit excess data from all the document# scenarios
* general
    - [ ] clean up SQL generation to prevent injection
    - [ ] how to handle a target value adjustment, which currency is it in?
    - [ ] **the sales data has to have a column for module and change ID, live sales data isn't going to work well**
        - [ ] need to target the live sales data, build build a whole new table to use it plus add version columns

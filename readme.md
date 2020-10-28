## worked on so far

setup
----------------------------------------------------------------------------------------------------------------------------------------------------
the basic assumption is a single sales table is available to work with that has a lot of related data that came from master data tables originally.
the goal then is to break that back apart to whatever degree is necessary.

* _**run**_ `schema.sql` and `perd.sql` to setup basic tables
* create a table fc.live as copied from target
* _**run**_ `target_info.sql` to populate the `fc.target_meta` table that holds all the columns and their roles
* fill in flags on table `fc.target_meta` to show how the data is related
* _**run**_ `build_master_tables.sql` to generate foreign key based master data


baseline
----------------------------------------------------------------------------------------------------------------------------------------------------
* copy history and increment by year to form a baseline
    * need to be able to handle order/ship dates generically


select x.*  from fc.live x
where
    (row_to_json(x)::jsonb) = $$
{
    "chan": "DIR",
    "account": "H&A MASTRONARDI",
    "shipgrp": "H&A MASTRONARDI"
}$$::jsonb

CREATE LENS nation_clean_idx AS SELECT * FROM nation WITH REPAIR_KEY('n_nationkey');
CREATE LENS supp_clean_idx AS SELECT * FROM supp WITH REPAIR_KEY('s_suppkey');


CREATE LENS cust_clean_idx AS SELECT * FROM cust WITH REPAIR_KEY('c_custkey');

-- CREATE LENS lineitem_clean AS SELECT * FROM lineitem WITH REPAIR_KEY('tid');
CREATE LENS orders_clean_idx AS SELECT * FROM orders WITH REPAIR_KEY('o_orderkey');


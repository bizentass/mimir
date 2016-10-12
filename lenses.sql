CREATE LENS nation_clean AS SELECT * FROM nation WITH REPAIR_KEY('tid');
CREATE LENS supplier_clean AS SELECT * FROM supplier WITH REPAIR_KEY('tid');


CREATE LENS cust_clean AS SELECT * FROM cust WITH REPAIR_KEY('tid');


CREATE LENS lineitem_clean AS SELECT * FROM lineitem WITH REPAIR_KEY('tid');
CREATE LENS orders_clean AS SELECT * FROM orders WITH REPAIR_KEY('tid');


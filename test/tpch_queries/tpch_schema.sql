create table Part(
	p_partkey INTEGER, 
  p_name VARCHAR(55), 
  p_mfgr CHAR(25), 
  p_brand CHAR(10), 
  p_type VARCHAR(25), 
  p_size INTEGER, 
  p_container CHAR(10), 
  p_retailprice real, 
  p_comment VARCHAR(23)
);

create table Supplier(
	s_suppKey INTEGER, 
  s_name CHAR(25), 
  s_address VARCHAR(40), 
  s_nationkey INTEGER, 
  s_phone CHAR(15), 
  s_acctbal REAL, 
  s_comment VARCHAR(101)
);

create table PartSupp(
	ps_partKey INTEGER, 
  ps_suppKey INTEGER, 
  ps_availqty INTEGER, 
  ps_supplycost REAL, 
  ps_comment VARCHAR(199)
);

create table Customer(
  c_custKey INTEGER, 
  c_name VARCHAR(25), 
  c_address VARCHAR(40), 
  c_nationkey INTEGER, 
  c_phone CHAR(15), 
  c_acctbal REAL, 
  c_mktsegment CHAR(10), 
  c_comment VARCHAR(117)
);

create table Nation(
	n_nationkey  INTEGER, 
  n_name CHAR(25), 
  n_regionkey INTEGER, 
  n_comment VARCHAR(152)
);

create table Region(
	r_regionkey INTEGER, 
  r_name CHAR(25), 
  r_comment VARCHAR(152)
);

create table LineItem(
	li_orderKey INTEGER, 
  li_partKey INTEGER, 
  li_suppKey INTEGER, 
  li_lineNumber INTEGER, 
  li_quantity INTEGER, 
  li_extendedPrice REAL, 
  li_discount REAL, 
  li_tax REAL, 
  li_returnFlag CHAR(1), 
  li_lineStatus CHAR(1), 
  li_shipDate DATE, 
  li_commitDate DATE, 
  li_receiptDate DATE, 
  li_shipInstruct CHAR(25), 
  li_shipMode CHAR(10), 
  li_comment VARCHAR(44)
);

create table Orders(
	o_orderKey INTEGER, 
  o_custKey INTEGER, 
  o_orderStatus CHAR(1), 
  o_totalPrice REAL, 
  o_orderDate DATE, 
  o_orderPriority CHAR(15), 
  o_clerk CHAR(15), 
  o_shipPriority INTEGER, 
  o_comment VARCHAR(79)
);

-- create index PartPKI on Part(partkey);
-- create index SupplierPK on Supplier(suppkey);
-- create index PartSuppPKI on PartSupp(partkey, suppkey);
-- create index CustomerPKI on Customer(custkey);
-- create index NationPKI on Nation(nationkey);
-- create index RegionPKI on Region(regionkey);
-- create index LineItemPKI on LineItem(orderkey, lineNumber);
-- create index OrdersPKI on Orders(orderkey);
-- 
-- create index SupplierNations on Supplier(nationkey);
-- create index CustomerNations on Customer(nationkey);
-- create index LineItemParts on LineItem(partkey);
-- create index LineItemSuppliers on LineItem(suppkey);
-- create index OrderCustomers on Orders(custKey);
-- create index PartSuppSupp on PartSupp(suppkey);

-- create index OrderDate on Orders(orderDate);
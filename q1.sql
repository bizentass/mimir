select o_orderkey, o_orderdate, o_shippriority 
--	from cust, orders, lineitem
        from orders_clean_idx, lineitem_clean, cust_clean_idx
	where c_mktsegment = 'BUILDING'
	  and c_custkey = o_custkey 
	  and o_orderkey = l_orderkey
	  and o_orderdate > DATE('1995-03-15')
	  and l_shipdate < DATE('1995-03-17');

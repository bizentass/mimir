SELECT n.name AS nation, 
     o.orderdate AS o_year,
     ((l.extendedprice * (1 - l.discount)) - (ps.supplycost * l.quantity))
        AS amount
FROM   part p, supplier s, lineitem l, partsupp ps, orders o, nation n
WHERE  s.suppkey = l.suppkey
AND  ps.suppkey = l.suppkey 
AND  ps.partkey = l.partkey
AND  p.partkey = l.partkey
AND  o.orderkey = l.orderkey 
AND  s.nationkey = n.nationkey 
AND  (p.name LIKE '%green%')

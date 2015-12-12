select c.C_NAME, o.O_ORDERPRIORITY, cast(l.L_COMMITDate as date), cast(l.L_SHIPDate as date) from lineitem l, orders o, customer c  where l.L_SHIPMODE = 'TRUCK' and ((cast(l.L_SHIPDate as date) between date '1996-06-10' and date '1996-06-15') or (cast(l.L_SHIPDate as date) between date '1996-05-10' and date '1996-05-15') or (cast(l.L_SHIPDate as date) between date '1996-04-10' and date '1996-04-15') or (cast(l.L_SHIPDate as date) between date '1996-03-10' and date '1996-03-15') or (cast(l.L_SHIPDate as date) between date '1996-02-10' and date '1996-02-15') or (cast(l.L_SHIPDate as date) between date '1996-01-10' and date '1996-01-15')) and (l.L_DISCOUNT > 0.01 and l.L_DISCOUNT < 0.05) and (l.L_TAX >0.01 and l.L_TAX <0.05) and substr(l._id,3) < 10000 and cast(o.O_ORDERKEY as varchar(10)) = cast(l.L_ORDERKEY as varchar(10)) and cast(o.O_CUSTKEY as varchar(10)) = cast(c.C_CUSTKEY as varchar(10)) and o.O_ORDERPRIORITY like '%4%';

#!/bin/sh

sf=$1

mvn exec:java -Dexec.mainClass="com.mapr.db.utils.ImportCSV" -Dexec.args="/maprdb/json/tpch/sf$sf/region /home/MAPRTECH/qa/abhishek/data/text/tpch/SF$sf/region.dat | /root/repos/drill-maprdb-tests/src/resources/tpch/schemas/region.schema"
mvn exec:java -Dexec.mainClass="com.mapr.db.utils.ImportCSV" -Dexec.args="/maprdb/json/tpch/sf$sf/nation /home/MAPRTECH/qa/abhishek/data/text/tpch/SF$sf/nation.dat | /root/repos/drill-maprdb-tests/src/resources/tpch/schemas/nation.schema"
mvn exec:java -Dexec.mainClass="com.mapr.db.utils.ImportCSV" -Dexec.args="/maprdb/json/tpch/sf$sf/part /home/MAPRTECH/qa/abhishek/data/text/tpch/SF$sf/part.dat | /root/repos/drill-maprdb-tests/src/resources/tpch/schemas/part.schema"
mvn exec:java -Dexec.mainClass="com.mapr.db.utils.ImportCSV" -Dexec.args="/maprdb/json/tpch/sf$sf/supplier /home/MAPRTECH/qa/abhishek/data/text/tpch/SF$sf/supplier.dat | /root/repos/drill-maprdb-tests/src/resources/tpch/schemas/supplier.schema"
mvn exec:java -Dexec.mainClass="com.mapr.db.utils.ImportCSV" -Dexec.args="/maprdb/json/tpch/sf$sf/partsupp /home/MAPRTECH/qa/abhishek/data/text/tpch/SF$sf/partsupp.dat | /root/repos/drill-maprdb-tests/src/resources/tpch/schemas/partsupp.schema"
mvn exec:java -Dexec.mainClass="com.mapr.db.utils.ImportCSV" -Dexec.args="/maprdb/json/tpch/sf$sf/customer /home/MAPRTECH/qa/abhishek/data/text/tpch/SF$sf/customer.dat | /root/repos/drill-maprdb-tests/src/resources/tpch/schemas/customer.schema"
mvn exec:java -Dexec.mainClass="com.mapr.db.utils.ImportCSV" -Dexec.args="/maprdb/json/tpch/sf$sf/orders /home/MAPRTECH/qa/abhishek/data/text/tpch/SF$sf/orders.dat | /root/repos/drill-maprdb-tests/src/resources/tpch/schemas/orders.schema"
mvn exec:java -Dexec.mainClass="com.mapr.db.utils.ImportCSV" -Dexec.args="/maprdb/json/tpch/sf$sf/lineitem /home/MAPRTECH/qa/abhishek/data/text/tpch/SF$sf/lineitem.dat | /root/repos/drill-maprdb-tests/src/resources/tpch/schemas/lineitem.schema"

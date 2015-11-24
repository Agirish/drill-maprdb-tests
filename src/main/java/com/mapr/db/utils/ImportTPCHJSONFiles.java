/*
 *  Copyright 2009-2015 MapR Technologies
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.mapr.db.utils;

import com.mapr.db.Condition;
import com.mapr.db.MapRDB;
import com.mapr.db.DBDocument;
import com.mapr.db.Mutation;
import com.mapr.db.Table;
import com.mapr.db.exceptions.DocumentExistsException;
import com.mapr.db.samples.basic.model.User;
import org.ojai.DocumentStream;

import javax.print.Doc;
import java.io.IOException;
import java.sql.Date;
import java.util.Arrays;
import java.util.Iterator;

import java.io.FileReader;
import java.io.File;
import java.util.Iterator;

import java.util.StringTokenizer;

import java.util.Scanner;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class ImportTPCHJSONFiles {

    public static final String CUSTOMER_JSON_TABLE_PATH = "/maprdb/json/tpch/customer";
    public static final String LINEITEM_JSON_TABLE_PATH = "/maprdb/json/tpch/lineitem";
    public static final String ORDERS_JSON_TABLE_PATH = "/maprdb/json/tpch/orders";
    public static final String PART_JSON_TABLE_PATH = "/maprdb/json/tpch/part";
    public static final String PARTSUPP_JSON_TABLE_PATH = "/maprdb/json/tpch/partsupp";
    public static final String SUPPLIER_JSON_TABLE_PATH = "/maprdb/json/tpch/supplier";
    public static final String REGION_JSON_TABLE_PATH = "/maprdb/json/tpch/region";

    public static final String CUSTOMER_JSON_FILE_PATH = "/root/tpch/customer.json";
    public static final String LINEITEM_JSON_FILE_PATH = "/root/tpch/lineitem.json";
    public static final String ORDERS_JSON_FILE_PATH = "/root/tpch/orders.json";
    public static final String PART_JSON_FILE_PATH = "/root/tpch/part.json";
    public static final String PARTSUPP_JSON_FILE_PATH = "/root/tpch/partsupp.json";
    public static final String SUPPLIER_JSON_FILE_PATH = "/root/tpch/supplier.json";
    public static final String REGION_JSON_FILE_PATH = "/root/tpch/region.json";

    public Table c_table;
    public Table l_table;
    public Table o_table;
    public Table p_table;
    public Table ps_table;
    public Table s_table;
    public Table r_table;

    public Customer c;
    public Lineitem l;
    public Orders o;
    public Part p;
    public Partsupp ps;
    public Supplier s;
    public Region r;

    public static int c_count = 0;
    public static int l_count = 0;
    public static int o_count = 0;
    public static int p_count = 0;
    public static int ps_count = 0;
    public static int s_count = 0;
    public static int r_count = 0;

    public ImportTPCHJSONFiles() {
    }

    public static void main(String[] args) throws Exception {

        ImportTPCHJSONFiles reader = new ImportTPCHJSONFiles();
        reader.readFileAndWriteToTable("CUSTOMER", CUSTOMER_JSON_FILE_PATH, CUSTOMER_JSON_TABLE_PATH);
    //reader.readFileAndWriteToTable(LINEITEM_JSON_FILE_PATH, LINEITEM_JSON_TABLE_PATH);
        //reader.readFileAndWriteToTable(ORDERS_JSON_FILE_PATH, ORDERS_JSON_TABLE_PATH);
        //reader.readFileAndWriteToTable(PART_JSON_FILE_PATH, PART_JSON_TABLE_PATH);
        //reader.readFileAndWriteToTable(PARTSUPP_JSON_FILE_PATH, PARTSUPP_JSON_TABLE_PATH);
        //reader.readFileAndWriteToTable(SUPPLIER_JSON_FILE_PATH, SUPPLIER_JSON_TABLE_PATH);
        //reader.readFileAndWriteToTable(REGION_JSON_FILE_PATH, REGION_JSON_TABLE_PATH);

    }

    public void readFileAndWriteToTable(String tpchTable, String file, String table) {

        try {
            Scanner scan = new Scanner(new File(file));
            String jsonFile = "";
            while (scan.hasNext()) {
                jsonFile += scan.next();
            }

            StringTokenizer st = new StringTokenizer(jsonFile, "%");
            String record = "";

            while (st.hasMoreTokens()) {

                record = st.nextToken();
		//System.out.println(record);

                JSONParser parser = new JSONParser();

                try {
                    Object obj = parser.parse(record);
                    JSONObject jsonObject = (JSONObject) obj;

                    if (tpchTable.equalsIgnoreCase("Customer")) {

                        c = new Customer();
                        c = c.getDocument(jsonObject);
                        c_table = this.getTable(c_table, CUSTOMER_JSON_TABLE_PATH);
                        c.insertDocument(c_table);
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Table getTable(Table table, String tableName) throws IOException {

        if (!MapRDB.tableExists(tableName)) {
            table = MapRDB.createTable(tableName); // Create table if not exists 
        } else {
            table = MapRDB.getTable(tableName);
        }
        return table;
    }

    public void deleteTable(String tableName) throws IOException {
        if (MapRDB.tableExists(tableName)) {
            MapRDB.deleteTable(tableName);
        }

    }

}

class Customer {

    String _id;
    Long c_custkey;
    String c_name;
    String c_address;
    Long c_nationkey;
    String c_phone;
    Double c_acctbal;
    String c_mktsegment;
    String c_comment;

    public String toString() {
        return "Customer\n\t" + c_custkey + "\n\t" + c_name + "\n\t" + c_address + "\n\t" + c_nationkey + "\n\t" + c_phone + "\n\t"
                + c_acctbal + "\n\t" + c_mktsegment + "\n\t" + c_comment + "\n";
    }

    public Customer getDocument(JSONObject jsonObject) {

        this._id = (String) "" + ImportTPCHJSONFiles.c_count++;
        this.c_custkey = (Long) jsonObject.get("C_CUSTKEY");
        this.c_name = (String) jsonObject.get("C_NAME");
        this.c_address = (String) jsonObject.get("C_ADDRESS");
        this.c_nationkey = (Long) jsonObject.get("C_NATIONKEY");
        this.c_phone = (String) jsonObject.get("C_PHONE");
        this.c_acctbal = (Double) jsonObject.get("C_ACCTBAL");
        this.c_mktsegment = (String) jsonObject.get("C_MKTSEGMENT");
        this.c_comment = (String) jsonObject.get("C_COMMENT");

        return this;
    }

    public void insertDocument(Table t) {
        DBDocument document = MapRDB.newDocument()
                .set("_id", this._id)
                .set("C_CUSTKEY", this.c_custkey)
                .set("C_NAME", this.c_name)
                .set("C_ADDRESS", this.c_address)
                .set("C_NATIONKEY", this.c_nationkey)
                .set("C_PHONE", this.c_phone)
                .set("C_ACCTBAL", this.c_acctbal)
                .set("C_MKTSEGMENT", this.c_mktsegment)
                .set("C_COMMENT", this.c_comment);
        t.insertOrReplace(document);
    }
}

class Lineitem {

    String _id;
    Long l_orderkey;
    Long l_partkey;
    Long l_suppkey;
    Long l_linenumber;
    Double l_quantity;
    Double l_extendedprice;
    Double l_discount;
    Double l_tax;
    String l_returnflag;
    String l_linestatus;
    String l_shipdate;
    String l_commitdate;
    String l_receiptdate;
    String l_shipinstruct;
    String l_shipmode;
    String l_comment;

    public String toString() {
        return "Lineitem\n\t" + l_orderkey + "\n\t" + l_partkey + "\n\t" + l_suppkey + "\n\t" + l_linenumber + "\n\t" + l_quantity + "\n\t"
                + l_extendedprice + "\n\t" + l_discount + "\n\t" + l_tax + "\n\t" + l_returnflag + "\n\t" + l_linestatus + "\n\t" + l_shipdate + "\n\t"
                + l_commitdate + "\n\t" + l_receiptdate + "\n\t" + l_shipinstruct + "\n\t" + l_shipmode + "\n\t" + l_comment + "\n";
    }
}

class Orders {

    String _id;
    Integer o_orderkey;
    Integer o_custkey;
    String o_orderstatus;
    Double o_totalprice;
    String o_orderdate;
    String o_orderpriority;
    String o_clerk;
    Integer o_shippriority;
    String o_comment;

    public String toString() {
        return "Orders\n\t" + o_orderkey + "\n\t" + o_custkey + "\n\t" + o_orderstatus + "\n\t" + o_totalprice + "\n\t" + o_orderdate + "\n\t"
                + o_orderpriority + "\n\t" + o_clerk + "\n\t" + o_shippriority + "\n\t" + o_comment + "\n";
    }
}

class Partsupp {

    String _id;
    Long ps_partkey;
    Long ps_suppkey;
    Long ps_availqty;
    Double ps_supplycost;
    String ps_comment;

    public String toString() {
        return "Partsupp\n\t" + ps_partkey + "\n\t" + ps_suppkey + "\n\t" + ps_availqty + "\n\t" + ps_supplycost + "\n\t" + ps_comment + "\n";
    }
}

class Supplier {

    String _id;
    Long s_suppkey;
    String s_name;
    String s_address;
    Long s_nationkey;
    String s_phone;
    Double s_acctbal;
    String s_comment;

    public String toString() {
        return "Supplier\n\t" + s_suppkey + "\n\t" + s_name + "\n\t" + s_address + "\n\t" + s_nationkey + "\n\t" + s_phone + "\n\t"
                + s_acctbal + "\n\t" + s_comment + "\n";
    }
}

class Part {

    String _id;
    Long p_partkey;
    String p_name;
    String p_mfgr;
    String p_brand;
    String p_type;
    Long p_size;
    String p_container;
    Double p_retailprice;
    String p_comment;

    public String toString() {
        return "Part\n\t" + p_partkey + "\n\t" + p_name + "\n\t" + p_mfgr + "\n\t" + p_brand + "\n\t" + p_type + "\n\t"
                + p_size + "\n\t" + p_container + "\n\t" + p_retailprice + "\n\t" + p_comment + "\n";
    }
}

class Nation {

    String _id;
    Long n_nationkey;
    String n_name;
    Long n_regionkey;
    String n_comment;

    public String toString() {
        return "Nation\n\t" + n_nationkey + "\n\t" + n_name + "\n\t" + n_regionkey + "\n\t" + n_comment + "\n";
    }
}

class Region {

    String _id;
    Long r_regionkey;
    String r_name;
    String r_comment;

    public String toString() {
        return "Region\n\t" + r_regionkey + "\n\t" + r_name + "\n\t" + r_comment + "\n";
    }
}


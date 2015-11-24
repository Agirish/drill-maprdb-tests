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

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.log4j.BasicConfigurator;

public class ImportTPCHJSONFiles {

    public static final String CUSTOMER_JSON_TABLE_PATH = "/maprdb/json/tpch/customer";
    public static final String LINEITEM_JSON_TABLE_PATH = "/maprdb/json/tpch/lineitem";
    public static final String ORDERS_JSON_TABLE_PATH = "/maprdb/json/tpch/orders";
    public static final String PART_JSON_TABLE_PATH = "/maprdb/json/tpch/part";
    public static final String PARTSUPP_JSON_TABLE_PATH = "/maprdb/json/tpch/partsupp";
    public static final String NATION_JSON_TABLE_PATH = "/maprdb/json/tpch/nation";
    public static final String SUPPLIER_JSON_TABLE_PATH = "/maprdb/json/tpch/supplier";
    public static final String REGION_JSON_TABLE_PATH = "/maprdb/json/tpch/region";

    public static final String CUSTOMER_JSON_FILE_PATH = "/root/tpch/customer.json";
    public static final String LINEITEM_JSON_FILE_PATH = "/root/tpch/lineitem.json";
    public static final String ORDERS_JSON_FILE_PATH = "/root/tpch/orders.json";
    public static final String PART_JSON_FILE_PATH = "/root/tpch/part.json";
    public static final String PARTSUPP_JSON_FILE_PATH = "/root/tpch/partsupp.json";
    public static final String NATION_JSON_FILE_PATH = "/root/tpch/nation.json";
    public static final String SUPPLIER_JSON_FILE_PATH = "/root/tpch/supplier.json";
    public static final String REGION_JSON_FILE_PATH = "/root/tpch/region.json";

    public Table c_table;
    public Table l_table;
    public Table o_table;
    public Table p_table;
    public Table ps_table;
    public Table n_table;
    public Table s_table;
    public Table r_table;

    public Customer c;
    public Lineitem l;
    public Orders o;
    public Part p;
    public Partsupp ps;
    public Nation n;
    public Supplier s;
    public Region r;

    public static long c_count = 0;
    public static long l_count = 0;
    public static long o_count = 0;
    public static long p_count = 0;
    public static long ps_count = 0;
    public static long n_count = 0;
    public static long s_count = 0;
    public static long r_count = 0;

    public ImportTPCHJSONFiles() {
    }

    static Logger logger = Logger.getLogger(ImportTPCHJSONFiles.class);

    public static void main(String[] args) throws Exception {

	BasicConfigurator.configure();
	Logger.getRootLogger().setLevel(Level.ERROR);

        ImportTPCHJSONFiles reader = new ImportTPCHJSONFiles();
        reader.readFileAndWriteToTable("CUSTOMER", CUSTOMER_JSON_TABLE_PATH, CUSTOMER_JSON_FILE_PATH);
        reader.readFileAndWriteToTable("LINEITEM", LINEITEM_JSON_TABLE_PATH, LINEITEM_JSON_FILE_PATH);
        reader.readFileAndWriteToTable("ORDERS", ORDERS_JSON_TABLE_PATH, ORDERS_JSON_FILE_PATH);
        reader.readFileAndWriteToTable("PART", PART_JSON_TABLE_PATH, PART_JSON_FILE_PATH);
        reader.readFileAndWriteToTable("PARTSUPP", PARTSUPP_JSON_TABLE_PATH, PARTSUPP_JSON_FILE_PATH);
        reader.readFileAndWriteToTable("NATION", NATION_JSON_TABLE_PATH, NATION_JSON_FILE_PATH);
        reader.readFileAndWriteToTable("SUPPLIER", SUPPLIER_JSON_TABLE_PATH, SUPPLIER_JSON_FILE_PATH);
        reader.readFileAndWriteToTable("REGION", REGION_JSON_TABLE_PATH, REGION_JSON_FILE_PATH);

    }

    public void readFileAndWriteToTable(String maprdbJsonTableName, String maprdbJsonTablePath, String jsonFilePath) {


	System.out.println("Importing "+maprdbJsonTableName);

        try {
            Scanner scan = new Scanner(new FileReader(jsonFilePath));
            StringBuilder jsonFileContents = new StringBuilder();
            while (scan.hasNextLine()) {
                jsonFileContents.append(scan.nextLine());
            }
	    scan.close();
            StringTokenizer st = new StringTokenizer(jsonFileContents.toString(), "%");
            String record = "";

            while (st.hasMoreTokens()) {

                record = st.nextToken();
		//System.out.println(record);
                JSONParser parser = new JSONParser();

                try {
                    Object obj = parser.parse(record);
                    JSONObject jsonObject = (JSONObject) obj;

                    if (maprdbJsonTableName.equalsIgnoreCase("Customer")) {

                        c = new Customer();
                        c = c.getDocument(jsonObject);
			//System.out.println(c.toString());
                        c_table = this.getTable(c_table, maprdbJsonTablePath);
                        c.insertDocument(c_table);
                    }
		    else if (maprdbJsonTableName.equalsIgnoreCase("Lineitem")) {
		
			l = new Lineitem();
			l = l.getDocument(jsonObject);
			//System.out.println(l.toString());
			l_table = this.getTable(l_table, maprdbJsonTablePath);
			l.insertDocument(l_table);
		    }
                    else if (maprdbJsonTableName.equalsIgnoreCase("Orders")) {

                        o = new Orders();
                        o = o.getDocument(jsonObject);
                        //System.out.println(o.toString());
                        o_table = this.getTable(o_table, maprdbJsonTablePath);
                        o.insertDocument(o_table);
                    }
                    else if (maprdbJsonTableName.equalsIgnoreCase("Part")) {

                        p = new Part();
                        p = p.getDocument(jsonObject);
                        //System.out.println(p.toString());
                        p_table = this.getTable(p_table, maprdbJsonTablePath);
                        p.insertDocument(p_table);
                    }
                    else if (maprdbJsonTableName.equalsIgnoreCase("Partsupp")) {

                        ps = new Partsupp();
                        ps = ps.getDocument(jsonObject);
                        //System.out.println(ps.toString());
                        ps_table = this.getTable(ps_table, maprdbJsonTablePath);
                        ps.insertDocument(ps_table);
                    }
                    else if (maprdbJsonTableName.equalsIgnoreCase("Nation")) {

                        n = new Nation();
                        n = n.getDocument(jsonObject);
                        //System.out.println(n.toString());
                        n_table = this.getTable(n_table, maprdbJsonTablePath);
                        n.insertDocument(n_table);
                    }
                    else if (maprdbJsonTableName.equalsIgnoreCase("Supplier")) {

                        s = new Supplier();
                        s = s.getDocument(jsonObject);
                        //System.out.println(s.toString());
                        s_table = this.getTable(s_table, maprdbJsonTablePath);
                        s.insertDocument(s_table);
                    }
                    else if (maprdbJsonTableName.equalsIgnoreCase("Region")) {

                        r = new Region();
                        r = r.getDocument(jsonObject);
                        //System.out.println(r.toString());
                        r_table = this.getTable(r_table, maprdbJsonTablePath);
                        r.insertDocument(r_table);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
	
	switch (maprdbJsonTableName){

		case "CUSTOMER": 
			System.out.println("Imported "+c_count+" Records\n");
			break;
		case "LINEITEM":
			System.out.println("Imported "+l_count+" Records\n");
                        break;
                case "ORDERS":
			System.out.println("Imported "+o_count+" Records\n");
                        break;
                case "PART":
			System.out.println("Imported "+p_count+" Records\n");
                        break;
                case "PARTSUPP":
			System.out.println("Imported "+ps_count+" Records\n");
                        break;
                case "NATION":
                        System.out.println("Imported "+n_count+" Records\n");
                        break;
                case "SUPPLIER":
                        System.out.println("Imported "+s_count+" Records\n");
                        break;
		case "REGION":
                        System.out.println("Imported "+r_count+" Records\n");
                        break;	       
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
    Long l_quantity;
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

    public Lineitem getDocument(JSONObject jsonObject) {

        this._id = (String) "" + ImportTPCHJSONFiles.l_count++;
        this.l_orderkey = (Long) jsonObject.get("L_ORDERKEY");
        this.l_partkey = (Long) jsonObject.get("L_PARTKEY");
        this.l_suppkey = (Long) jsonObject.get("L_SUPPKEY");
        this.l_linenumber = (Long) jsonObject.get("L_LINENUMBER");
        this.l_quantity = (Long) jsonObject.get("L_QUANTITY");
        this.l_extendedprice = (Double) jsonObject.get("L_EXTENDEDPRICE");
        this.l_discount = (Double) jsonObject.get("L_DISCOUNT");
        this.l_tax = (Double) jsonObject.get("L_TAX");
	this.l_returnflag = (String) jsonObject.get("L_RETURNFLAG");
	this.l_linestatus = (String) jsonObject.get("L_LINESTATUS");
        this.l_shipdate = (String) jsonObject.get("L_SHIPDATE");
        this.l_commitdate = (String) jsonObject.get("L_COMMITDATE");
        this.l_receiptdate = (String) jsonObject.get("L_RECEIPTDATE");
        this.l_shipinstruct = (String) jsonObject.get("L_SHIPINSTRUCT");
        this.l_shipmode = (String) jsonObject.get("L_SHIPMODE");
        this.l_comment = (String) jsonObject.get("L_COMMENT");

        return this;
    }

    public void insertDocument(Table t) {
        DBDocument document = MapRDB.newDocument()
                .set("_id", this._id)
                .set("L_ORDERKEY", this.l_orderkey)
                .set("L_PARTKEY", this.l_partkey)
                .set("L_SUPPKEY", this.l_suppkey)
                .set("L_LINENUMBER", this.l_linenumber)
                .set("L_QUANTITY", this.l_quantity)
                .set("L_EXTENDEDPRICE", this.l_extendedprice)
                .set("L_DISCOUNT", this.l_discount)
                .set("L_TAX", this.l_tax)
                .set("L_RETURNFLAG", this.l_returnflag)
                .set("L_LINESTATUS", this.l_linestatus)
                .set("L_SHIPDATE", this.l_shipdate)
                .set("L_COMMITDATE", this.l_commitdate)
                .set("L_RECEIPTDATE", this.l_receiptdate)
                .set("L_SHIPINSTRUCT", this.l_shipinstruct)
                .set("L_SHIPMODE", this.l_shipmode)
                .set("L_COMMENT", this.l_comment);
        t.insertOrReplace(document);
    }

}

class Orders {

    String _id;
    Long o_orderkey;
    Long o_custkey;
    String o_orderstatus;
    Double o_totalprice;
    String o_orderdate;
    String o_orderpriority;
    String o_clerk;
    String o_shippriority;
    String o_comment;

    public String toString() {
        return "Orders\n\t" + o_orderkey + "\n\t" + o_custkey + "\n\t" + o_orderstatus + "\n\t" + o_totalprice + "\n\t" + o_orderdate + "\n\t"
                + o_orderpriority + "\n\t" + o_clerk + "\n\t" + o_shippriority + "\n\t" + o_comment + "\n";
    }

    public Orders getDocument(JSONObject jsonObject) {
        
        this._id = (String) "" + ImportTPCHJSONFiles.o_count++;
        this.o_orderkey = (Long) jsonObject.get("O_ORDERKEY");
        this.o_custkey = (Long) jsonObject.get("O_CUSTKEY");
        this.o_orderstatus = (String) jsonObject.get("O_ORDERSTATUS");
        this.o_totalprice = (Double) jsonObject.get("O_TOTALPRICE");
        this.o_orderdate = (String) jsonObject.get("O_ORDERDATE");
        this.o_orderpriority = (String) jsonObject.get("O_ORDERPRIORITY");
        this.o_clerk = (String) jsonObject.get("O_CLERK");
        this.o_shippriority = (String) jsonObject.get("O_SHIPPRIORITY");
	this.o_comment = (String) jsonObject.get("O_COMMENT");	

	return this;
    }

    public void insertDocument(Table t) {
        DBDocument document = MapRDB.newDocument()
                .set("_id", this._id)
                .set("O_ORDERKEY", this.o_orderkey)
                .set("O_CUSTKEY", this.o_custkey)
                .set("O_ORDERSTATUS", this.o_orderstatus)
                .set("O_TOTALPRICE", this.o_totalprice)
                .set("O_ORDERDATE", this.o_orderdate)
                .set("O_ORDERPRIORITY", this.o_orderpriority)
                .set("O_CLERK", this.o_clerk)
                .set("O_SHIPPRIORITY", this.o_shippriority)
		.set("O_COMMENT", this.o_comment);
	t.insertOrReplace(document);
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

    public Partsupp getDocument(JSONObject jsonObject) {

        this._id = (String) "" + ImportTPCHJSONFiles.ps_count++;
        this.ps_partkey = (Long) jsonObject.get("PS_PARTKEY");
        this.ps_suppkey = (Long) jsonObject.get("PS_SUPPKEY");
        this.ps_availqty = (Long) jsonObject.get("PS_AVAILQTY");
        this.ps_supplycost = (Double) jsonObject.get("PS_SUPPLYCOST");
        this.ps_comment = (String) jsonObject.get("PS_COMMENT");

        return this;
    }

    public void insertDocument(Table t) {
        DBDocument document = MapRDB.newDocument()
                .set("_id", this._id)
                .set("PS_PARTKEY", this.ps_partkey)
                .set("PS_SUPPKEY", this.ps_suppkey)
                .set("PS_AVAILQTY", this.ps_availqty)
                .set("PS_SUPPLYCOST", this.ps_supplycost)
                .set("PS_COMMENT", this.ps_comment);
        t.insertOrReplace(document);
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

    public Supplier getDocument(JSONObject jsonObject) {

        this._id = (String) "" + ImportTPCHJSONFiles.s_count++;
        this.s_suppkey = (Long) jsonObject.get("S_SUPPKEY");
        this.s_name = (String) jsonObject.get("S_NAME");
        this.s_address = (String) jsonObject.get("S_ADDRESS");
        this.s_nationkey = (Long) jsonObject.get("S_NATIONKEY");
        this.s_phone = (String) jsonObject.get("S_PHONE");
        this.s_acctbal = (Double) jsonObject.get("S_ACCTBAL");
        this.s_comment = (String) jsonObject.get("S_COMMENT");

        return this;
    }

    public void insertDocument(Table t) {
        DBDocument document = MapRDB.newDocument()
                .set("_id", this._id)
                .set("S_SUPPKEY", this.s_suppkey)
                .set("S_NAME", this.s_name)
                .set("S_ADDRESS", this.s_address)
                .set("S_NATIONKEY", this.s_nationkey)
                .set("S_PHONE", this.s_phone)
                .set("S_ACCTBAL", this.s_acctbal)
                .set("S_COMMENT", this.s_comment);
        t.insertOrReplace(document);
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

    public Part getDocument(JSONObject jsonObject) {

        this._id = (String) "" + ImportTPCHJSONFiles.p_count++;
        this.p_partkey = (Long) jsonObject.get("P_PARTKEY");
        this.p_name = (String) jsonObject.get("P_NAME");
        this.p_mfgr = (String) jsonObject.get("P_MFGR");
        this.p_brand = (String) jsonObject.get("P_BRAND");
        this.p_type = (String) jsonObject.get("P_TYPE");
        this.p_size = (Long) jsonObject.get("P_SIZE");
        this.p_container = (String) jsonObject.get("P_CONTAINER");
        this.p_retailprice = (Double) jsonObject.get("P_RETAILPRICE");
        this.p_comment = (String) jsonObject.get("P_COMMENT");

        return this;
    }

    public void insertDocument(Table t) {
        DBDocument document = MapRDB.newDocument()
                .set("_id", this._id)
                .set("P_PARTKEY", this.p_partkey)
                .set("P_NAME", this.p_name)
                .set("P_MFGR", this.p_mfgr)
                .set("P_BRAND", this.p_brand)
                .set("P_TYPE", this.p_type)
                .set("P_SIZE", this.p_size)
                .set("P_CONTAINER", this.p_container)
                .set("P_RETAILPRICE", this.p_retailprice)
                .set("P_COMMENT", this.p_comment);
        t.insertOrReplace(document);
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

    public Nation getDocument(JSONObject jsonObject) {

        this._id = (String) "" + ImportTPCHJSONFiles.n_count++;
        this.n_nationkey = (Long) jsonObject.get("N_NATIONKEY");
        this.n_name = (String) jsonObject.get("N_NAME");
        this.n_regionkey = (Long) jsonObject.get("N_REGIONKEY");
        this.n_comment = (String) jsonObject.get("N_COMMENT");

        return this;
    }

    public void insertDocument(Table t) {
        DBDocument document = MapRDB.newDocument()
                .set("_id", this._id)
                .set("N_NATIONKEY", this.n_nationkey)
                .set("N_NAME", this.n_name)
                .set("N_REGIONKEY", this.n_regionkey)
                .set("N_COMMENT", this.n_comment);
        t.insertOrReplace(document);
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

    public Region getDocument(JSONObject jsonObject) {

        this._id = (String) "" + ImportTPCHJSONFiles.r_count++;
        this.r_regionkey = (Long) jsonObject.get("R_REGIONKEY");
        this.r_name = (String) jsonObject.get("R_NAME");
        this.r_comment = (String) jsonObject.get("R_COMMENT");

        return this;
    }

    public void insertDocument(Table t) {
        DBDocument document = MapRDB.newDocument()
                .set("_id", this._id)
                .set("R_REGIONKEY", this.r_regionkey)
                .set("R_NAME", this.r_name)
                .set("R_COMMENT", this.r_comment);
        t.insertOrReplace(document);
    }
}


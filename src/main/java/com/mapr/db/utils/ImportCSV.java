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

import com.mapr.db.MapRDB;
import com.mapr.db.DBDocument;
import com.mapr.db.Table;

import java.io.IOException;
import java.io.FileReader;

import java.sql.Date;
import java.util.*;

import org.apache.commons.lang.StringUtils;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.log4j.BasicConfigurator;

public class ImportCSV {

    static Logger logger = Logger.getLogger(ImportTPCHJSONFiles.class);

    private static String schemaFile = "";
    private static String csvFile = "";
    private static String delimiter = ",";
    private static String maprdbTablePath = "";
    private static Table maprdbTable = null;

    private static long idcounter = 0l;

    private static int countColumnsInSchema = 0;
    private static ArrayList<String> valueTypesInSchema = new ArrayList<>();
    private static ArrayList<String> columnNamesInSchema = new ArrayList<>();

    public static void main(String[] args) throws Exception {

        if (args.length != 4) {
            System.out.println("MapR-DB JSON Tables - Import CSV"
                    + "\nUsage:\n"
                    + "\tParam 1: JSON Table Path (MapR-FS)\n"
                    + "\tParam 2: Text File Path (Local-FS)\n"
                    + "\tParam 3: Text File Delimiter (Local-FS)\n"
                    + "\tParam 4: Schema File Path (Local-FS)\n");

            System.exit(-1);
        }

        maprdbTablePath = args[0];
        csvFile = args[1];
        delimiter = args[2];
        schemaFile = args[3];

        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.ERROR);

        ImportCSV imp = new ImportCSV();

        imp.readSchema(schemaFile);
        imp.printSchema();
        imp.readAndImportCSV(csvFile, delimiter);

        System.out.println("Successfully inserted " + idcounter + " documents from "
                + csvFile + " into " + maprdbTablePath);
    }

    public void readSchema(String path) {

        String schemaLine = "";
        StringTokenizer st;
        String column = "", datatype = "";

        countColumnsInSchema = 0;

        try {
            Scanner scan = new Scanner(new FileReader(path));

            while (scan.hasNextLine()) {
                schemaLine = scan.nextLine().trim();
                if (schemaLine.startsWith("#"))
                    continue;
                st = new StringTokenizer(schemaLine, " ");
                while (st.hasMoreTokens()) {
                    column = st.nextToken();
                    datatype = st.nextToken();
                    valueTypesInSchema.add(countColumnsInSchema, datatype);
                    columnNamesInSchema.add(countColumnsInSchema, column);
                }
                countColumnsInSchema++;
            }
            scan.close();
        } catch (Exception e) {
            System.out.println("Error reading schema: " + schemaLine +
                    "\n(Column Name:" + column + "\tData type:" + datatype + ")");
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public void printSchema() {

        System.out.println("Schema:");
        for (int i = 0; i < valueTypesInSchema.size(); i++) {
            System.out.println(columnNamesInSchema.get(i) + " " + valueTypesInSchema.get(i));
        }
    }

    public void readAndImportCSV(String path, String delimiter) {

        String dataLine = "";
        String[] dataList;
        String data = "";

        ArrayList<Object> values = new ArrayList<>();
        int countColumnsInData = 0;

        try {
            Scanner scan = new Scanner(new FileReader(path));

            while (scan.hasNextLine()) {
                countColumnsInData = 0;
                dataLine = scan.nextLine().trim();
                //System.out.println(dataLine);
                if (dataLine.endsWith(delimiter)) {
                    dataLine = dataLine.substring(0, dataLine.length() - 1);
                }
                dataList = StringUtils.splitPreserveAllTokens(dataLine, delimiter);

                for (int i = 0; i < dataList.length; i++) {

                    data = dataList[i];
                    if (data.isEmpty()) {
                        if (valueTypesInSchema.get(i) == "String") {
                            data = "null";
                        } else {
                            data = "0";
                        }
                    }

                    switch (valueTypesInSchema.get(i).toLowerCase()) {
                        case "int":
                        case "integer":
                        case "bigint":
                        case "long":
                            values.add(countColumnsInData, Long.valueOf(data));
                            break;
                        case "float":
                        case "double":
                            values.add(countColumnsInData, Double.valueOf(data));
                            break;
                        case "date":
                            values.add(countColumnsInData, Date.valueOf(data));
                            break;
                        default:
                            values.add(countColumnsInData, String.valueOf(data));
                            break;
                    }

                    //System.out.println("Inserting " + values.get(countColumnsInData)
                    //        + " into column " + columnNamesInSchema.get(countColumnsInData));

                    countColumnsInData++;
                }
                insertDocument(values, countColumnsInData, maprdbTable, maprdbTablePath);
            }
            scan.close();
        } catch (Exception e) {
            System.out.println("Error importing text:\n\t" + dataLine + "\ninto\n\t" + maprdbTablePath);
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private Table getTable(Table table, String tablePath) throws IOException {

        if (!MapRDB.tableExists(tablePath)) {
            table = MapRDB.createTable(tablePath); // Create table if not exists
        } else {
            table = MapRDB.getTable(tablePath);
        }
        return table;
    }

    private void insertDocument(ArrayList<Object> values, int countColumnsInData, Table table, String tablePath) {

        if (countColumnsInData != countColumnsInSchema) {
            System.out.println("Provided schema cannot be used to import the text data. " +
                    "\nThe number of columns in schema (" + countColumnsInSchema
                    + ") does not match columns in data (" + countColumnsInData + ")");
            System.exit(-1);
        }
        try {

            Table t = getTable(table, tablePath);
            DBDocument document = MapRDB.newDocument();
            //document.set("_id", getNextID());

            for (int i = 0; i < countColumnsInData; i++) {

                //System.out.println(columnNamesInSchema.get(i) + " | " + values.get(i)
                //        + " | " + valueTypesInSchema.get(i));

                switch (valueTypesInSchema.get(i).toLowerCase()) {
                    case "int":
                    case "integer":
                    case "bigint":
                    case "long":
                        document.set(columnNamesInSchema.get(i), (Long) values.get(i));
                        break;
                    case "float":
                    case "double":
                        document.set(columnNamesInSchema.get(i), (Double) values.get(i));
                        break;
                    case "date":
                        document.set(columnNamesInSchema.get(i), (Date) values.get(i));
                        break;
                    default:
                        document.set(columnNamesInSchema.get(i), (String) values.get(i));
                        break;
                }
            }
            //t.insert(document); // Bug?
            //t.insertOrReplace(document);
            String ID = getNextID();
            t.insertOrReplace(ID, document);
            t.flush();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private String getNextID() {
        idcounter++;

        if (idcounter >= 10000 && idcounter % 10000 == 0)
            System.out.println("Inserted " + idcounter + " Documents\n");

        //System.out.println("\nDocument ID: " + idcounter);
        return "ID" + idcounter;
    }

}

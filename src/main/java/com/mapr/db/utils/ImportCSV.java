package com.mapr.db.utils;

import com.mapr.db.MapRDB;
import com.mapr.db.DBDocument;
import com.mapr.db.Table;

import java.io.IOException;
import java.io.FileReader;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.StringTokenizer;
import java.util.Scanner;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.log4j.BasicConfigurator;

public class ImportCSV {

    static Logger logger = Logger.getLogger(ImportTPCHJSONFiles.class);

    private static LinkedHashMap<String, String> schemaList = new LinkedHashMap<>();
    private static String schemaFile = "";
    private static String csvFile = "", delimiter = ",";
    private static String maprdbTablePath = "";
    private static Table maprdbTable = null;
    private static long idcounter = 0l;
    private static int countColumnsInSchema = 0;
    private static int countColumnsInData = 0;
    private static ArrayList<String> values = new ArrayList<>();
    private static ArrayList<String> valueTypes = new ArrayList<>();
    private static ArrayList<String> columnNames = new ArrayList<>();

    public static void main(String[] args) throws Exception {

        if (args.length != 4) {
            System.out.println("MapR-DB JSON Tables - Import CSV\nUsage:\n"
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
        //imp.printSchema(schemaList);
        imp.readAndImportCSV(csvFile, delimiter);
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
                    schemaList.put(column, datatype);
                    valueTypes.add(countColumnsInSchema, datatype);
                    columnNames.add(countColumnsInSchema, column);
                }
                countColumnsInSchema++;
            }
            scan.close();
        } catch (Exception e) {
            System.out.println("Error reading schema: " + schemaLine + "\n(Column Name:" + column + "\tData type:" + datatype + ")");
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public void printSchema(LinkedHashMap<String, String> schemaList) {

        for (String columns : schemaList.keySet()) {
            System.out.println(columns + " " + schemaList.get(columns));
        }
    }

    public void readAndImportCSV(String path, String delimiter) {

        String dataLine = "";
        StringTokenizer st;
        String data = "";

        try {
            Scanner scan = new Scanner(new FileReader(path));

            while (scan.hasNextLine()) {
                countColumnsInData = 0;
                dataLine = scan.nextLine().trim();
                st = new StringTokenizer(dataLine, delimiter);
                while (st.hasMoreTokens()) {
                    data = st.nextToken();
                    if (data.isEmpty())
                        data = "null";
                    values.add(countColumnsInData, data);
                    //System.out.println("Inserting " + values.get(countColumnsInData) + " into column " + columnNames.get(countColumnsInData));
                    countColumnsInData++;
                }
                insertDocument(maprdbTable, maprdbTablePath);
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

    private void insertDocument(Table table, String tablePath) {

        if (countColumnsInData != countColumnsInSchema) {
            System.out.println("Provided schema cannot be used to import the text data. " +
                    "\nThe number of columns in schema (" + countColumnsInSchema + ") does not match columns in data (" + countColumnsInData + ")");
            System.exit(-1);
        }
        try {
            Table t = getTable(table, tablePath);
            DBDocument document = MapRDB.newDocument();
            document.set("_id", getNextID());
            for (int i = 0; i < countColumnsInData; i++) {
                //System.out.println(columnNames.get(i) + " | " + values.get(i));
                document.set(columnNames.get(i), values.get(i));
            }
            t.insertOrReplace(document);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String getNextID() {
        idcounter++;
        //System.out.println("\nDocument ID: " + idcounter);
        return "" + idcounter;
    }

}

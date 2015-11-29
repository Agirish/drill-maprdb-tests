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
    static LinkedHashMap<String, String> schemaList = new LinkedHashMap<>();
    public static String schemaFile = "";
    public static String csvFile = "";
    public static String maprdbTablePath = "";
    public static Table maprdbTable = null;
    public static long idcounter = 0l;
    public static ArrayList<String> values = new ArrayList<>();
    static ArrayList<String> valueTypes = new ArrayList<>();
    static ArrayList<String> columnNames = new ArrayList<>();

    public static void main(String[] args) throws Exception {

        if (args.length != 3){
            System.out.println("MapR-DB JSON Tables - Import CSV\nUsage:\n"
                    + "\tParam 1: JSON Table Path (MapR-FS)\n"
                    + "\tParam 2: Text File Path (Local-FS)\n"
                    + "\tParam 3: Schema File Path (Local-FS)\n");
            System.exit(-1);
        }

        maprdbTablePath=args[0];
        csvFile=args[1];
        schemaFile =args[2];

        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.ERROR);

        ImportCSV imp = new ImportCSV();

        imp.readSchema(schemaFile);
        imp.readAndImportCSV(csvFile);
    }

    public void readSchema(String path) {

        try {
            Scanner scan = new Scanner(new FileReader(path));
            String schemaLine;
            StringTokenizer st;
            String column = "", datatype = "";
            int countColumns = 0;

            while (scan.hasNextLine()) {
                schemaLine = scan.nextLine();
                st = new StringTokenizer(schemaLine, " ");
                while (st.hasMoreTokens()) {
                    column = st.nextToken();
                    datatype = st.nextToken();
                    schemaList.put(column, datatype);
                    valueTypes.add(countColumns, datatype);
                    columnNames.add(countColumns, column);
                }
            }
            scan.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void printSchema() {

        for (String columns : schemaList.keySet()) {
            System.out.println(columns + " " + schemaList.get(columns));
        }
    }

    public void readAndImportCSV(String path) {

        try {
            Scanner scan = new Scanner(new FileReader(path));
            String dataLine;
            StringTokenizer st;

            int count = 0;

            while (scan.hasNextLine()) {
                count = 0;
                dataLine = scan.nextLine();
                st = new StringTokenizer(dataLine, ",");
                while (st.hasMoreTokens()) {
                    values.add(count, st.nextToken());
                    //System.out.println(values.get(count));
                    count++;
                }
                insertDocument(count, maprdbTable, maprdbTablePath);
            }
            scan.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Table getTable(Table table, String tablePath) throws IOException {

        if (!MapRDB.tableExists(tablePath)) {
            table = MapRDB.createTable(tablePath); // Create table if not exists
        } else {
            table = MapRDB.getTable(tablePath);
        }
        return table;
    }

    public void insertDocument(int count, Table table, String tablePath) {

        System.out.println("Inserting " + count + " documents");
        try {
            Table t = getTable(table, tablePath);
            DBDocument document = MapRDB.newDocument();
            document.set("_id", getNextID());
            for (int i = 0; i < count; i++) {
                System.out.println(columnNames.get(i) + " | " + values.get(i));
                document.set(columnNames.get(i), values.get(i));
            }
            t.insertOrReplace(document);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getNextID() {
        idcounter++;
        System.out.println("\nDocument ID: " + idcounter);
        return "" + idcounter;
    }

}

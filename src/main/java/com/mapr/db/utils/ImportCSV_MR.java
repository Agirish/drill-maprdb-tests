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

import com.mapr.db.Admin;
import com.mapr.db.DBDocument;
import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import com.mapr.db.mapreduce.ByteBufWritableComparable;
import com.mapr.db.mapreduce.TableOutputFormat;
import com.mapr.db.rowcol.DBDocumentImpl;

import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.ojai.Document;
import org.ojai.Value;
import org.ojai.json.mapreduce.JSONDocumentSerialization;

import java.io.FileReader;
import java.io.IOException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.StringTokenizer;

public class ImportCSV_MR extends Configured implements Tool {

    static Logger logger = Logger.getLogger(ImportTPCHJSONFiles.class);

    private static Configuration conf = null;

    public static String schemaFile = "";
    public static String inputDir = "";
    public static String delimiter = ",";
    public static String outputTable = "";
    public static Table maprdbTable = null;

    public static int countColumnsInSchema = 0;
    public static ArrayList<String> valueTypesInSchema = new ArrayList<>();
    public static ArrayList<String> columnNamesInSchema = new ArrayList<>();


    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        conf = new Configuration();
        System.exit(ToolRunner.run(conf, new ImportCSV_MR(), args));
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 4) {
            System.out.println("MapR-DB JSON Tables - Import CSV"
                    + "\nUsage:\n"
                    + "\tParam 1: JSON Table Path (MapR-FS)\n"
                    + "\tParam 2: Text File Path (Local-FS)\n"
                    + "\tParam 3: Text File Delimiter (Local-FS)\n"
                    + "\tParam 4: Schema File Path (Local-FS)\n");

            System.exit(-1);
        }

        outputTable = args[0].toString().trim();
        inputDir = args[1].toString().trim();
        delimiter = args[2].toString().trim();
        schemaFile = args[3].toString().trim();

        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.ERROR);

        ImportCSV_MR imp = new ImportCSV_MR();

        imp.readSchema(schemaFile);
        imp.printSchema();

        Job job = Job.getInstance(conf, "ImportCSV_MR");
        job.setJarByClass(ImportCSV_MR.class);

        job.setMapperClass(MyMapper.class);

        conf = job.getConfiguration();
        conf.setStrings("io.serializations", new String[]
                {conf.get("io.serializations"), JSONDocumentSerialization.class.getName()});

        conf.set("countColumnsInSchema", String.valueOf(countColumnsInSchema));

        conf.set("delimiter", delimiter);

        conf.set("tablePath", outputTable);

        String valueTypes[] = valueTypesInSchema.toArray(new String[valueTypesInSchema.size()]);
        conf.setStrings("valueTypesInSchema", valueTypes);

        String columnNames[] = columnNamesInSchema.toArray(new String[columnNamesInSchema.size()]);
        conf.setStrings("columnNamesInSchema", columnNames);

        //Deciding the appropriate Input format class along with their input path
        FileInputFormat.addInputPath(job, new Path(inputDir));
        job.setInputFormatClass(TextInputFormat.class);

        //Mapper output record key and value class
        job.setMapOutputKeyClass(ByteBufWritableComparable.class);
        job.setMapOutputValueClass(DBDocumentImpl.class);

        //Deciding the appropriate Output format class along with their input path
        conf.set("maprdb.mapred.outputtable", outputTable);
        job.setOutputFormatClass(TableOutputFormat.class);

        //Reducer output record key and value class
        job.setNumReduceTasks(0);

        boolean isJobSuccessful = job.waitForCompletion(true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
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
            logger.info(columnNamesInSchema.get(i) + " " + valueTypesInSchema.get(i));
        }
    }

}

class MyMapper extends Mapper<LongWritable, Text, Value, Document> {

    static Logger logger = Logger.getLogger(MyMapper.class);

    private long id;
    private int increment;

    Configuration conf;

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {

        super.setup(context);
        conf = context.getConfiguration();

        id = context.getTaskAttemptID().getTaskID().getId();
        increment = context.getConfiguration().getInt("mapred.map.tasks", 0);
        if (increment == 0) {
            throw new IllegalArgumentException("mapred.map.tasks is zero");
        }
    }

    public String getNextID() {
        id += increment;

        return "ID" + id;
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        logger.info("Processing " + value.toString());
        System.out.println("Processing " + value.toString());
        DBDocument document = readAndImportCSV(value.toString());
        Value id = document.getId();
        logger.info("ID: " + id.getString());
        System.out.println("ID: " + id.getString());
        context.write(id, document);

    }

    public void printSchema() {

        System.out.println("Schema:");
        for (int i = 0; i < conf.getStrings("valueTypesInSchema").length; i++) {
            System.out.println(conf.getStrings("columnNamesInSchema")[i] + " " + conf.getStrings("valueTypesInSchema")[i]);
            logger.info(conf.getStrings("columnNamesInSchema")[i] + " " + conf.getStrings("valueTypesInSchema")[i]);
        }
    }

    public DBDocument readAndImportCSV(String dataLine) {

        ArrayList<Object> values = new ArrayList<>();
        int countColumnsInData = 0;

        if (dataLine.endsWith(conf.get("delimiter"))) {
            dataLine = dataLine.substring(0, dataLine.length() - 1);
        }

        String[] dataList = StringUtils.splitPreserveAllTokens(dataLine, conf.get("delimiter"));

        for (int i = 0; i < dataList.length; i++) {

            logger.info("Processing column " + i + " off " + dataList.length);
            System.out.println("Processing column " + i + " off " + dataList.length);

            String data = dataList[i];

            logger.info("Processing data " + data);
            System.out.println("Processing data " + data);

            if (data.isEmpty()) {
                if (conf.getStrings("valueTypesInSchema")[i] == "String") {
                    data = "null";
                } else {
                    data = "0";
                }
            }

            printSchema();

            switch (conf.getStrings("valueTypesInSchema")[i].toLowerCase()) {
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

            countColumnsInData++;
        }
        return insertDocument(values, countColumnsInData, conf.get("tablePath"));
    }

    private DBDocument insertDocument(ArrayList<Object> values, int countColumnsInData, String tablePath) {

        DBDocument document = MapRDB.newDocument();

        try {

            Table t = getTable(tablePath);

            if (countColumnsInData != Integer.valueOf(conf.get("countColumnsInSchema"))) {
                System.out.println("Provided schema cannot be used to import the text data. " +
                        "\nThe number of columns in schema (" + Integer.valueOf(conf.get("countColumnsInSchema"))
                        + ") does not match columns in data (" + countColumnsInData + ")");
                System.exit(-1);
            }

            for (int i = 0; i < countColumnsInData; i++) {

                switch (conf.getStrings("valueTypesInSchema")[i].toLowerCase()) {
                    case "int":
                    case "integer":
                    case "bigint":
                    case "long":
                        document.set(conf.getStrings("columnNamesInSchema")[i], (Long) values.get(i));
                        break;
                    case "float":
                    case "double":
                        document.set(conf.getStrings("columnNamesInSchema")[i], (Double) values.get(i));
                        break;
                    case "date":
                        document.set(conf.getStrings("columnNamesInSchema")[i], (Date) values.get(i));
                        break;
                    default:
                        document.set(conf.getStrings("columnNamesInSchema")[i], (String) values.get(i));
                        break;
                }
            }

            document.set("_id", getNextID());

        } catch (Exception e) {
            e.printStackTrace();
        }
        return document;
    }

    private Table getTable(String tablePath) throws IOException {


        Admin maprAdmin = MapRDB.newAdmin();
        if (!maprAdmin.tableExists(tablePath)) {
            ImportCSV_MR.maprdbTable = maprAdmin.createTable(tablePath);
        }

       /* if (!MapRDB.tableExists(tablePath)) {
            ImportCSV_MR.maprdbTable = MapRDB.createTable(tablePath); // Create table if not exists
        } else {
            ImportCSV_MR.maprdbTable = MapRDB.getTable(tablePath);
        }*/
        return ImportCSV_MR.maprdbTable;
    }
}

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
package com.mapr.db.tests;

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

import static com.mapr.db.Condition.Op.EQUAL;
import static com.mapr.db.Condition.Op.GREATER_OR_EQUAL;
import static com.mapr.db.Condition.Op.LESS;

public class CreateJSONTable {

    public static final String TABLE_PATH = "/apps/user_profiles";

    private Table table;

    public CreateJSONTable() {
    }

    public static void main(String[] args) throws Exception {

        CreateJSONTable app = new CreateJSONTable();
        app.run();

    }

    private void run() throws Exception {

        this.deleteTable(TABLE_PATH);
        this.table = this.getTable(TABLE_PATH);
        this.printTableInformation(TABLE_PATH);

        this.table.close();

    }

    /**
     * Get the table, create it if not present
     *
     * @throws IOException
     */
    private Table getTable(String tableName) throws IOException {
        Table table;

        if (!MapRDB.tableExists(tableName)) {
            table = MapRDB.createTable(tableName); // Create the table if not already present
        } else {
            table = MapRDB.getTable(tableName); // get the table
        }
        return table;
    }

    private void deleteTable(String tableName) throws IOException {
        if (MapRDB.tableExists(tableName)) {
            MapRDB.deleteTable(tableName);
        }

    }

    /**
     * Print table information such as Name, Path and Tablets information
     * (sharding)
     *
     * @param tableName
     * @throws IOException
     */
    private void printTableInformation(String tableName) throws IOException {
        Table table = MapRDB.getTable(tableName);
        System.out.println("\n=============== TABLE INFO ===============");
        System.out.println(" Table Name : " + table.getName());
        System.out.println(" Table Path : " + table.getPath());
        System.out.println(" Table Infos : " + Arrays.toString(table.getTabletInfos()));
        System.out.println("==========================================\n");
    }
}


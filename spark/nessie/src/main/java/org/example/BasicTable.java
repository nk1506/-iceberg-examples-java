package org.example;

import static org.example.Constants.DEFAULT_CATALOG;
import static org.example.Constants.DEFAULT_TABLE_NAME;

public class BasicTable extends SparkS3Setup {

    public void run() {
        spark.sql("CREATE TABLE IF NOT EXISTS "+getTable()+" (id int)");
        spark.sql("INSERT INTO "+getTable()+" values(1)");
    }

    private String getTable() {
        return DEFAULT_CATALOG+"."+DEFAULT_TABLE_NAME;
    }

}
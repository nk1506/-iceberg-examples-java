package org.example;

import static org.example.Constants.DEFAULT_CATALOG;
import static org.example.Constants.DEFAULT_NAMESPACE;
import static org.example.Constants.DEFAULT_TABLE_NAME;

public class BasicTable extends SparkS3Setup {

    public void run() {
        spark.sql("CREATE NAMESPACE IF NOT EXISTS default");
        spark.sql("CREATE TABLE IF NOT EXISTS "+getTable()+" (id int)");
        spark.sql("INSERT INTO "+getTable()+" values(1)");

        Runnable r1 = () -> spark.sql("CALL "+DEFAULT_CATALOG+".system.remove_orphan_files(table => '"+DEFAULT_NAMESPACE+"."+DEFAULT_TABLE_NAME+"', older_than => TIMESTAMP '2023-10-07 00:00:00.000', dry_run => false)").collectAsList();
        try {
            r1.run();
            Thread.sleep(1000);
            Thread.sleep(10000);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    private String getTable() {
        return DEFAULT_CATALOG+"."+DEFAULT_NAMESPACE+"."+DEFAULT_TABLE_NAME;
    }

}
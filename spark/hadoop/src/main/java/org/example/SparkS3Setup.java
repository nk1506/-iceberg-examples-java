package org.example;

import static org.example.Constants.DEFAULT_CATALOG;
import static org.example.Constants.DEFAULT_WAREHOUSE;
import static org.example.Constants.S3_ACCESS_CREDENTIAL;
import static org.example.Constants.S3_ACCESS_KEY;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SparkS3Setup {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkS3Setup.class);
    protected static SparkSession spark;

    static {
        spark = SparkSession.builder()
                .appName("hadoop-catalog")
                .master("local[2]")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.testing", "true")
                .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
                .config("spark.hadoop.fs.s3a.secret.key", S3_ACCESS_CREDENTIAL)
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                .config("spark.hadoop.fs.s3a.fast.upload", "true")
                .config("spark.hadoop.fs.s3a.multipart.size", 104857600)
                .config("fs.s3a.connection.maximum", 100)
                .config("spark.sql.catalog."+DEFAULT_CATALOG+".io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                .config("spark.sql.catalog."+DEFAULT_CATALOG, "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog."+DEFAULT_CATALOG+".type", "hadoop")
                .config("spark.sql.catalog."+DEFAULT_CATALOG+".warehouse", DEFAULT_WAREHOUSE)
                .getOrCreate();
        LOGGER.info("Spark Version:{}", spark.version());
    }

}

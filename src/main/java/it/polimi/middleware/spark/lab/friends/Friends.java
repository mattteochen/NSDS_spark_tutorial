package it.polimi.middleware.spark.lab.friends;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import javax.xml.crypto.Data;

/**
 * Implement an iterative algorithm that implements the transitive closure of
 * friends
 * (people that are friends of friends of ... of my friends).
 *
 * Set the value of the flag useCache to see the effects of caching.
 */
public class Friends {
    static final List<StructField> fields = new ArrayList<>();
    private static final String inputCSV = "lab_files/friends/friends.csv";
    private static final boolean useCache = true;
    static SparkSession spark;
    static StructType schema;

    private static Dataset<Row> getTable(String filePath) {
        return spark
                .read()
                .option("header", "false")
                .option("delimiter", ",")
                .schema(schema)
                .csv(filePath + inputCSV);
    }

    public static void main(String[] args) {
        final String master = args.length > 0 ? args[0] : "local[4]";
        final String filePath = args.length > 1 ? args[1] : "./";
        final String appName = useCache ? "FriendsCache" : "FriendsNoCache";

        spark = SparkSession
                .builder()
                .master(master)
                .appName(appName)
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        fields.add(DataTypes.createStructField("person", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("friend", DataTypes.StringType, false));
        schema = DataTypes.createStructType(fields);
        
        //Retrieve original, left and right tables
        Dataset<Row> oldTable = getTable(filePath);
        Dataset<Row> df1 = oldTable;
        //Rename column because spark gives error when using aliases `df1 = df.as("df1"), df2 = df.as("df2") -> df1.join(df2)`.
        df1 = df1.withColumnRenamed("friend", "friend1");
        df1 = df1.withColumnRenamed("person", "person1");
        Dataset<Row> df2 = oldTable;
        df2 = df2.withColumnRenamed("friend", "friend2");
        df2 = df2.withColumnRenamed("person", "person2");
        Dataset<Row> na = df1.join(df2, df1.col("friend1").equalTo(df2.col("person2")), "inner")
            .select(df1.col("person1"), df2.col("friend2"));
        System.out.println("First new update:");
        na.show();

        //Create the updated table
        Dataset<Row> updatedTable = oldTable.union(na).distinct();
        System.out.println("First updated table:");
        updatedTable.show();

        System.out.println("Size cmp: " + updatedTable.count() + " " + oldTable.count());
        while (updatedTable.count() != oldTable.count()) {
            oldTable = updatedTable;
            df1 = updatedTable;
            df1 = df1.withColumnRenamed("friend", "friend1");
            df1 = df1.withColumnRenamed("person", "person1");
            df2 = updatedTable;
            df2 = df2.withColumnRenamed("friend", "friend2");
            df2 = df2.withColumnRenamed("person", "person2");
            na = df1.join(df2, df1.col("friend1").equalTo(df2.col("person2")), "inner")
                .select(df1.col("person1"), df2.col("friend2"));
            System.out.println("Update:");
            na.show();
            updatedTable = oldTable.union(na).distinct();
            System.out.println("New updated table:");
            updatedTable.show();
            if (useCache) {
                updatedTable.cache();
                oldTable.cache();
            }
            System.out.println("Size cmp: " + updatedTable.count() + " " + oldTable.count());
        }

        spark.close();
    }
}
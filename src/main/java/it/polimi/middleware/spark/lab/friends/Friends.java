package it.polimi.middleware.spark.lab.friends;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Implement an iterative algorithm that implements the transitive closure of friends
 * (people that are friends of friends of ... of my friends).
 *
 * Set the value of the flag useCache to see the effects of caching.
 */
public class Friends {
    private static final boolean useCache = true;

    public static void main(String[] args) {
        final String master = args.length > 0 ? args[0] : "local[4]";
        final String filePath = args.length > 1 ? args[1] : "./";
        final String appName = useCache ? "FriendsCache" : "FriendsNoCache";

        final SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName(appName)
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        final List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("person", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("friend", DataTypes.StringType, false));
        final StructType schema = DataTypes.createStructType(fields);

        final Dataset<Row> input = spark
                .read()
                .option("header", "false")
                .option("delimiter", ",")
                .schema(schema)
                .csv(filePath + "lab_files/friends/friends.csv");

        input.show();
        JavaRDD<Row> inputRdd = input.toJavaRDD();
        JavaPairRDD<String, String> friendInput =  inputRdd.mapToPair(
            e -> {
                Object a = e.get(0);
                Object b = e.get(1);

                String strA = (String)a;
                String strB = (String)b;

                return new Tuple2<String, String>(strA, strB);
            }
        );

        friendInput.foreach(e -> {
            System.out.println(e._1 + " " + e._2);
        });

        JavaRDD<Tuple2<String, List<String>>> emptyRDD = spark.sparkContext().emptyRDD(null);
        JavaPairRDD<String, List<String>> adjiacentList = 


        boolean changed = true;
        while (changed) {
            friendInput.map(e -> {
                String person = e._1;
                String friend = e._2;
                adjiacentList.set
            });
        }

        spark.close();
    }
}

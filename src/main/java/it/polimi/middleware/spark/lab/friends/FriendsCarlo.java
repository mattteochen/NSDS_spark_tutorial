package it.polimi.middleware.spark.lab.friends;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Implement an iterative algorithm that implements the transitive closure of friends
 * (people that are friends of friends of ... of my friends).
 *
 * Set the value of the flag useCache to see the effects of caching.
 */
public class FriendsCarlo {
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
        fields.add(DataTypes.createStructField("friendJoin", DataTypes.StringType, false));
        final StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> people = spark
                .read()
                .option("header", "false")
                .option("delimiter", ",")
                .schema(schema)
                .csv(filePath + "lab_files/friends/friends.csv");

        people.show();

        final List<StructField> fieldsFriends = new ArrayList<>();
        fieldsFriends.add(DataTypes.createStructField("friendJoin", DataTypes.StringType, false));
        fieldsFriends.add(DataTypes.createStructField("friend", DataTypes.StringType, false));
        final StructType schemaFriends = DataTypes.createStructType(fieldsFriends);

        Dataset<Row> friends = spark
                .read()
                .option("header", "false")
                .option("delimiter", ",")
                .schema(schemaFriends)
                .csv(filePath + "lab_files/friends/friends.csv");

        while (true) {
            Dataset<Row> newPeople = people.union(
                    people
                            .join(friends, friends.col("friendJoin").equalTo(people.col("friendJoin")))
                            .select("person", "friend")
                            .withColumnRenamed("friend", "friendJoin")
            )
                    .distinct()
                    .orderBy("person", "friendJoin");
            newPeople.show();
            if (newPeople.count() == people.count()) break;
            else people = newPeople;
        }

        spark.close();
    }
}

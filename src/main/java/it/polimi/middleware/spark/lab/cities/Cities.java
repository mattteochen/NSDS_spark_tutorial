package it.polimi.middleware.spark.lab.cities;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import static org.apache.spark.sql.functions.*;

public class Cities {
    public static void main(String[] args) throws TimeoutException {
        final String master = args.length > 0 ? args[0] : "local[4]";
        final String filePath = args.length > 1 ? args[1] : "./";

        final SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName("SparkEval")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        final List<StructField> citiesRegionsFields = new ArrayList<>();
        citiesRegionsFields.add(DataTypes.createStructField("city", DataTypes.StringType, false));
        citiesRegionsFields.add(DataTypes.createStructField("region", DataTypes.StringType, false));
        final StructType citiesRegionsSchema = DataTypes.createStructType(citiesRegionsFields);

        final List<StructField> citiesPopulationFields = new ArrayList<>();
        citiesPopulationFields.add(DataTypes.createStructField("id", DataTypes.IntegerType, false));
        citiesPopulationFields.add(DataTypes.createStructField("city", DataTypes.StringType, false));
        citiesPopulationFields.add(DataTypes.createStructField("population", DataTypes.IntegerType, false));
        final StructType citiesPopulationSchema = DataTypes.createStructType(citiesPopulationFields);

        final Dataset<Row> citiesPopulation = spark
                .read()
                .option("header", "true")
                .option("delimiter", ";")
                .schema(citiesPopulationSchema)
                .csv(filePath + "lab_files/cities/cities_population.csv");

        citiesPopulation.cache();

        final Dataset<Row> citiesRegions = spark
                .read()
                .option("header", "true")
                .option("delimiter", ";")
                .schema(citiesRegionsSchema)
                .csv(filePath + "lab_files/cities/cities_regions.csv");

        final Dataset<Row> joinedData = citiesPopulation
                .join(citiesRegions, citiesRegions.col("city").equalTo(citiesPopulation.col("city")))
                .select(citiesPopulation.col("id"), citiesRegions.col("region"), citiesPopulation.col("city"), citiesPopulation.col("population"))
                .sort("region");

        joinedData.cache();
        joinedData.show();

        final Dataset<Row> q1 =
            joinedData
            .groupBy("region")
            .sum("population");


        q1.show();

        final Dataset<Row> q2 = joinedData
                .groupBy("region")
                .agg(count("city"), max("population"));

        q2.show();

        // JavaRDD where each element is an integer and represents the population of a city
        JavaRDD<Integer> population = citiesPopulation.toJavaRDD().map(r -> r.getInt(2));

        var oldPopulation = population;
        population.cache();

        int populationSum = population.reduce(Integer::sum);
        int year = 0;
        while (populationSum < 100000000) {
            population = population
                    .map(p -> p > 1000? (int) (((float) p)*(1.01)) : (int)(((float) p)*(0.99)));
            population.cache();

            populationSum = population.reduce(Integer::sum);
            System.out.printf("Year: %d, total population: %d\n", year, populationSum);
            year++;

            oldPopulation.unpersist();
            oldPopulation = population;
        }

        // Bookings: the value represents the city of the booking
        final Dataset<Row> bookings = spark
                .readStream()
                .format("rate")
                .option("rowsPerSecond", 100)
                .load();

        final StreamingQuery q4 = bookings
                .join(joinedData, bookings.col("value").equalTo(joinedData.col("id")))
                .drop("population", "value")
                .groupBy(
                        window(col("timestamp"), "30 seconds", "5 seconds"),
                        col("region")
                )
                .sum()
                .writeStream()
                .outputMode("Update")
                .format("console")
                .start();

        try {
            q4.awaitTermination();
        } catch (final StreamingQueryException e) {
            e.printStackTrace();
        }

        spark.close();
    }
}
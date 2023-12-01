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

        final Dataset<Row> citiesRegions = spark
                .read()
                .option("header", "true")
                .option("delimiter", ";")
                .schema(citiesRegionsSchema)
                .csv(filePath + "lab_files/cities/cities_regions.csv");

        // TODO: add code here if necessary

        final Dataset<Row> q1 = 
            citiesPopulation
            .join(citiesRegions, citiesRegions.col("city").equalTo(citiesPopulation.col("city")))
            .groupBy("region")
            .sum("population");


        q1.show();

        final Dataset<Row> q2MaxPopulatedCityPerRegion = 
            citiesPopulation
            .join(citiesRegions, citiesRegions.col("city").equalTo(citiesPopulation.col("city")))
            .groupBy("region")
            .max("population").as("maxPopulated")
            .select("*");

        q2MaxPopulatedCityPerRegion.show();

        final Dataset<Row> q2CountCitiesPerRegion = 
            citiesPopulation
            .join(citiesRegions, citiesRegions.col("city").equalTo(citiesPopulation.col("city")))
            .groupBy("region")
            .count().as("numCities")
            .select("*");

        q2CountCitiesPerRegion.show();

        final Dataset<Row> q2 = 
            q2MaxPopulatedCityPerRegion
            .join(q2CountCitiesPerRegion, q2MaxPopulatedCityPerRegion.col("region").equalTo(q2CountCitiesPerRegion.col("region")))
            .select(q2MaxPopulatedCityPerRegion.col("region"), q2MaxPopulatedCityPerRegion.col("max(population)"), q2CountCitiesPerRegion.col("count"));
        
        q2.show();

        // JavaRDD where each element is an integer and represents the population of a city
        JavaRDD<Integer> population = citiesPopulation.toJavaRDD().map(r -> r.getInt(2));
        // TODO: add code here to produce the output for query Q3

        // Bookings: the value represents the city of the booking
        final Dataset<Row> bookings = spark
                .readStream()
                .format("rate")
                .option("rowsPerSecond", 100)
                .load();

        final StreamingQuery q4 = null; // TODO query Q4

        try {
            q4.awaitTermination();
        } catch (final StreamingQueryException e) {
            e.printStackTrace();
        }

        spark.close();
    }
}
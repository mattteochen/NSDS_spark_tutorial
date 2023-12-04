package it.polimi.nsds.spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

/*
 * Group number: 03
 *
 * Group members
 *  - Carlo Ronconi
 *  - Giulia Prosio
 *  - Kaixi Matteo Chen
 */

public class Solution {
    private static final int numCourses = 3000;

    public static void main(String[] args) throws TimeoutException {
        final String master = args.length > 0 ? args[0] : "local[4]";
        final String filePath = args.length > 1 ? args[1] : "./";

        final SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName("SparkEval")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        final List<StructField> profsFields = new ArrayList<>();
        profsFields.add(DataTypes.createStructField("prof_name", DataTypes.StringType, false));
        profsFields.add(DataTypes.createStructField("course_name", DataTypes.StringType, false));
        final StructType profsSchema = DataTypes.createStructType(profsFields);

        final List<StructField> coursesFields = new ArrayList<>();
        coursesFields.add(DataTypes.createStructField("course_name", DataTypes.StringType, false));
        coursesFields.add(DataTypes.createStructField("course_hours", DataTypes.IntegerType, false));
        coursesFields.add(DataTypes.createStructField("course_students", DataTypes.IntegerType, false));
        final StructType coursesSchema = DataTypes.createStructType(coursesFields);

        final List<StructField> videosFields = new ArrayList<>();
        videosFields.add(DataTypes.createStructField("video_id", DataTypes.IntegerType, false));
        videosFields.add(DataTypes.createStructField("video_duration", DataTypes.IntegerType, false));
        videosFields.add(DataTypes.createStructField("course_name", DataTypes.StringType, false));
        final StructType videosSchema = DataTypes.createStructType(videosFields);

        // Professors: prof_name, course_name
        final Dataset<Row> profs = spark
                .read()
                .option("header", "false")
                .option("delimiter", ",")
                .schema(profsSchema)
                .csv(filePath + "files/profs.csv");

        // Courses: course_name, course_hours, course_students
        final Dataset<Row> courses = spark
                .read()
                .option("header", "false")
                .option("delimiter", ",")
                .schema(coursesSchema)
                .csv(filePath + "files/courses.csv");

        // Videos: video_id, video_duration, course_name
        final Dataset<Row> videos = spark
                .read()
                .option("header", "false")
                .option("delimiter", ",")
                .schema(videosSchema)
                .csv(filePath + "files/videos.csv");

        // Visualizations: value, timestamp
        // value represents the video id
        final Dataset<Row> visualizations = spark
                .readStream()
                .format("rate")
                .option("rowsPerSecond", 10)
                .load()
                .withColumn("value", col("value").mod(numCourses));

        /**
         * TODO: Enter your code below
         */

        /**
         * Caches: These two tables are re used multiple times. 
         */
        videos.cache();
        courses.cache();

        /*
         * Query Q1. Compute the total number of lecture hours per prof
         */

        final Dataset<Row> q1 = profs
                .join(courses, profs.col("course_name").equalTo(courses.col("course_name")), "inner")
                .groupBy("prof_name")
                .sum("course_hours")
                .select("prof_name", "sum(course_hours)");

        q1.show();

        /*
         * Query Q2. For each course, compute the total duration of all the visualizations of videos of that course,
         * computed over a minute, updated every 10 seconds
         * 
         * Notes: The total hours of visualization for each course are computed
         * by summing the visualization durations of all the video's linked to
         * each course.
         */
        StreamingQuery q2 = visualizations 
                .join(videos, visualizations.col("value").equalTo(videos.col("video_id")))
                .groupBy(
                        col("course_name"),
                        window(col("timestamp"), "60 seconds", "10 seconds")
                )
                .sum("video_duration")
                .writeStream()
                .outputMode("update")
                .format("console")
                .start();

        /*
         * Query Q3. For each video, compute the total number of visualizations of that video
         * with respect to the number of students in the course in which the video is used.
         * 
         * Notes: In order to compute the total ratio 
         * (total_visualization_count / course_students) we compute the partial ratio upon receiving a
         * single visualization from the dynamic stream and then we sum then
         * together for each video_id.
         */
        final Dataset<Row> courseXvideos = courses
                .join(videos, videos.col("course_name").equalTo(courses.col("course_name")))
                .select(videos.col("course_name"), courses.col("course_students"), videos.col("video_id"))
                .withColumn("ones", lit(1));

        courseXvideos.cache();

        final StreamingQuery q3 = visualizations 
                .join(courseXvideos, visualizations.col("value").equalTo(courseXvideos.col("video_id")))
                .withColumn("ratios", courseXvideos.col("ones").divide(courseXvideos.col("course_students")))
                .groupBy(
                        col("video_id")
                )
                .sum("ratios")
                .withColumnRenamed("sum(ratios)", "visual_ratios")
                .writeStream()
                .outputMode("update")
                .format("console")
                .start();

        try {
            q2.awaitTermination();
            q3.awaitTermination();
        } catch (final StreamingQueryException e) {
            e.printStackTrace();
        }

        spark.close();
    }
}

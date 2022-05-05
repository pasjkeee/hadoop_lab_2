package bdtc.lab2;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.regex.Pattern;

/**
 * Считает количество событий syslog разного уровная log level по часам.
 */
@Slf4j
public class SparkSQLApplication {

    /**
     * @param args - args[0]: входной файл, args[1] - выходная папка
     */
    public static void main(String[] args) {
        if (args.length < 2) {
            throw new RuntimeException("Usage: java -jar SparkSQLApplication.jar input.file outputDirectory");
        }

        log.info("Appliction started!");
        log.debug("Application started");
        SparkSession sc = SparkSession
                .builder()
                .master("local")
                .appName("SparkSQLApplication")
                .getOrCreate();

        Dataset<String> df = sc.read().text(args[0]).as(Encoders.STRING());
        JavaRDD<String> lines = sc.read().textFile(args[0]).javaRDD();
        JavaRDD<String> lines2 = sc.read().textFile(args[1]).javaRDD();

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("Spark RDD Example using Java");
        sparkConf.setMaster("local[2]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> rd = sparkContext.textFile(args[0]);
        log.info("===============COUNTING...================");
        JavaRDD<Tuple2<String, String>> result = LogLevelEventCounter.countLogLevelPerHour(lines, lines2);
        log.info("============SAVING FILE TO " + args[2] + " directory============");
        result.saveAsTextFile(args[1]);
    }
}

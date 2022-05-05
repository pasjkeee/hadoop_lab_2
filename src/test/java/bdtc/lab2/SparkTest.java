package bdtc.lab2;

import lombok.var;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

import static bdtc.lab2.LogLevelEventCounter.countLogLevelPerHour;

public class SparkTest {
    //1 вход 0 выход

    final  String stringJournalIn1 = "MEPHI;1;25-06-2019 01:11:28;1";
    final  String stringJournalOut1 = "MEPHI;1;25-06-2019 09:11:28;0";
    final  String stringJournalIn2 = "MEPHI;2;25-06-2020 01:11:28;1";
    final  String stringJournalOut2 = "MEPHI;2;25-06-2020 10:11:28;0";
    final  String stringJournalIn3 = "PUPI;3;25-06-2019 01:11:28;1";
    final  String stringJournalOut3 = "PUPI;3;25-06-2019 10:11:28;0";
    final  String stringJournalIn4 = "MEPHI;4;25-06-2020 01:11:28;1";
    final  String stringJournalOut4 = "MEPHI;4;25-06-2020 10:11:28;0";
    final  String stringJournalIn5 = "MEPHI;4;26-06-2020 01:11:28;1";
    final  String stringJournalOut5 = "MEPHI;4;26-06-2020 10:11:28;0";

    final  String stringReestr1 = "MEPHI;4;Pisec;25-06-2020";
    final  String stringReestr5 = "MEPHI;4;Pisec;25-07-2020";
    final  String stringReestr2 = "PUPI;4;Lolec;25-06-2019";
    final  String stringReestr4 = "MEPHI;1;Kotec;25-06-2019";
    final  String stringReestr3 = "MEPHI;3;Momec;25-06-2020";

    SparkSession ss = SparkSession
            .builder()
            .master("local")
            .appName("SparkSQLApplication")
            .getOrCreate();

    @Test
    public void testOneLog() {

        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());;
        JavaRDD<String> myDudu = sc.parallelize(Arrays.asList(stringJournalIn1, stringJournalOut1, stringJournalIn2, stringJournalOut2,
                stringJournalIn3,stringJournalOut3, stringJournalIn4, stringJournalOut4, stringJournalIn5, stringJournalOut5  ));
        JavaRDD<String> myDudu2 = sc.parallelize(Arrays.asList(stringReestr1,stringReestr2, stringReestr3, stringReestr4, stringReestr5 ));
        JavaRDD<Tuple2<String, String>> result = countLogLevelPerHour(myDudu, myDudu2);
        List<Tuple2<String, String>> rowList = result.collect();

        var test1 = rowList.get(0);
        assert test1._1().equals("4/[MEPHI/2020");
        assert test1._2().equals("1080 2");

        var test2 = rowList.get(1);
        assert test2._1().equals("1/[MEPHI/2019");
        assert test2._2().equals("480 1");

        var test3 = rowList.get(2);
        assert test3._1().equals("3/[PUPI/2019");
        assert test3._2().equals("540 0");

        var test4 = rowList.get(3);
        assert test4._1().equals("2/[MEPHI/2020");
        assert test4._2().equals("540 0");
    }
}

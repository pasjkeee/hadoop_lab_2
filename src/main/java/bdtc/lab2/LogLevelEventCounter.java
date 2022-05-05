package bdtc.lab2;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Serializable;
import scala.Tuple2;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.*;

import static java.time.temporal.ChronoField.*;

@AllArgsConstructor
@Slf4j
public class LogLevelEventCounter implements Serializable {

    // Формат времени логов - н-р, 'Oct 26 13:54:06'
    private static DateTimeFormatter formatter = new DateTimeFormatterBuilder()
            .appendPattern("dd-MM-yyyy HH:mm:ss")
            .toFormatter();

    private static DateTimeFormatter formatterD = new DateTimeFormatterBuilder()
            .appendPattern("dd-MM-yyyy")
            .parseDefaulting(MINUTE_OF_HOUR, 0)
            .parseDefaulting(SECOND_OF_MINUTE, 0)
            .parseDefaulting(HOUR_OF_DAY, 0)
            .toFormatter();

    public static JavaRDD<Tuple2<String, String>> countLogLevelPerHour(JavaRDD<String> lol, JavaRDD<String> lol2) {
//        Dataset<String> words = inputDataset.map(s -> Arrays.toString(s.split("\n")), Encoders.STRING());

        //вход выход
        JavaRDD<String> word = lol.map(s -> {
            return Arrays.toString(s.split("\n"));});

        //работы
        JavaRDD<String> word2 = lol2.map(s -> {
            return Arrays.toString(s.split("\n"));});

        //вход выход выводим в формате userId/университет/год -> полная строка
        JavaPairRDD<String, String> ones = word.mapToPair(s -> {
            String[] vars = s.split(";");
            String user = vars[1];
            String university = vars[0];
            int year = LocalDateTime.parse(vars[2], formatter).getYear();
            return new Tuple2<String, String>(user + "/" + university + "/" + year, s);
        });

        //выводим работы в формате userId/университет/год -> 1
        JavaPairRDD<String,Integer> twos = word2.mapToPair(s -> {
            String[] vars = s.split(";");
            String university = vars[0];
            int year = LocalDateTime.parse(vars[3].substring(0, vars[3].length()-1), formatterD).getYear();

            return new Tuple2<String,Integer>(vars[1]+"/"+ university + "/" + year, 1);
        });

        //суммируем все строки кторые есть суммарно [MEPHI;4;25-06-2020 01:11:28;1]_[MEPHI;4;25-06-2020 10:11:28;0]_[MEPHI;4;26-06-2020 01:11:28;1]
        JavaPairRDD<String, String> counts = ones.reduceByKey((a, b) -> {
            System.out.println(a);
            System.out.println(b);
            return a + "_" + b;
        });

        // суммируем количество работ для уникального ключа
        JavaPairRDD<String, Integer> counts2 = twos.reduceByKey((a, b) -> {
            return a + b;
        });


        // выводим разницу в минутах между входом и выходом подряд двух
        JavaRDD<Tuple2<String, Long>> countss = counts.map((s) -> {
            String[] vars = s._2().split("_");

            long diff = 0;
            for (int i = 0; i < vars.length - 1; i+=2) {
                LocalDateTime date1 = LocalDateTime.parse(vars[i].split(";")[2], formatter);
                LocalDateTime date2 = LocalDateTime.parse(vars[i+1].split(";")[2], formatter);
                diff += Duration.between(date1, date2).toMinutes();
            }

            return new Tuple2<String, Long>(s._1(), diff);

        });

        JavaRDD<Tuple2<String, Integer>> countss2 = counts2.map((s) -> {
            return new Tuple2<String, Integer>(s._1(), s._2());
        });


        countss.foreach(t -> {
            System.out.println(t._1() + ": " + t._2());
        });

        System.out.println("--------------------------------");

        countss2.foreach(t -> {
            System.out.println(t._1() + ": " + t._2());
        });

        List<Tuple2<String, Integer>> coll1 = countss2.collect();
//        List<Tuple2<String, Integer>> coll2 = countss2.collect();



        // сопоставляем выходные минуты и количество работ
        JavaRDD<Tuple2<String, String>> countsss = countss.map(t -> {

           long coun = coll1.stream().filter(p -> Objects.equals(p._1(), t._1())).mapToLong(w -> w._2()).sum();

            return new Tuple2<String, String>(t._1(), t._2() + " " + coun);
        });

        System.out.println("--------------------------------");

        countsss.foreach(t -> {
            System.out.println(t._1() + ": " + t._2());
        });



//        Dataset<LogLevelHour> logLevelHourDataset = words.map(s -> {
//
//            String[] logFields = s.split(",");
//            LocalDateTime date = LocalDateTime.parse(logFields[2], formatter);
//
//            return new LogLevelHour(logFields[1], date.getHour());
//            }, Encoders.bean(LogLevelHour.class))
//                .coalesce(1);
//
//        // Группирует по значениям часа и уровня логирования
//        Dataset<Row> t = logLevelHourDataset.groupBy("hour", "logLevel")
//                .count()
//                .toDF("hour", "logLevel", "num")
//                // сортируем по времени лога - для красоты
//                .sort(functions.asc("hour"));
//        log.info("===========RESULT=========== ");
//        t.show();
//        System.out.println(t.collect().toString());

        return countsss;
    }

}

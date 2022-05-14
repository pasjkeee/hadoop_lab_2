Centos 7

Java 1.8

Maven

hdfs

```

if [ ! -f spark-2.3.1-bin-hadoop2.7.tgz ]; then 
    wget https://archive.apache.org/dist/spark/spark-2.3.1/spark-2.3.1-bin-hadoop2.7.tgz
    tar xvzf spark-2.3.1-bin-hadoop2.7.tgz
else
    echo "Spark already exists, skipping..."
fi

export PATH=$PATH:/usr/local/hadoop/bin/
hadoop dfs -rm -r input
hadoop dfs -rm -r out
export PATH=$PATH:/sqoop-1.4.7.bin__hadoop-2.6.0/bin
export SPARK_HOME=/spark-2.3.1-bin-hadoop2.7
export HADOOP_CONF_DIR=$HADOOP_PREFIX/etc/hadoop
export PATH=$PATH:/spark-2.3.1-bin-hadoop2.7/bin

```


Запуск сборки проекта

```
git clone https://github.com/pasjkeee/hadoop_lab_2.git
cd hadoop_lab_2
mvn clean install
```

Приложение принимает на вход 2 файла:

1) Журнал входа-выхода
<img width="639" alt="image" src="https://user-images.githubusercontent.com/72603507/168401492-e178eb9d-68dd-4518-bbb0-8ad77822ebf2.png">

2) Реестр опубликованных работ
<img width="668" alt="image" src="https://user-images.githubusercontent.com/72603507/168401517-bd0d4f44-fcab-471c-a4b3-a09eb1b96328.png">

Генерация происходит с помошью скрипта

<img width="870" alt="image" src="https://user-images.githubusercontent.com/72603507/168401545-3536563a-6a4b-46c4-8166-e9f0a383b803.png">

Загружаем сгенерированный input в hdfs

```
/opt/$HADOOP_HOME/sbin/start-dfs.sh
hdfs dfs -put input
```

![telegram-cloud-document-2-5474131574771423319](https://user-images.githubusercontent.com/72603507/168402921-36719b81-93e5-45b6-a3cd-f5421c149ffa.jpg)

Далее эти файлы считываются в программе
<img width="909" alt="image" src="https://user-images.githubusercontent.com/72603507/168401687-22d08e0d-f1ad-4957-8e40-3684277b6c7b.png">

Журнал входа-выхода редьюсится в JavaRDD по шаблону [key: humanId/univercity/year]: spenTimeInYear
Реестр опубликованных работ редьюсится в JavaRDD по шаблону [key: humanId/univercity/year]: numOfWorks

И далее она матчатся по ключу и выводятся в файл

Ход работы программы

![telegram-cloud-document-2-5474131574771423322](https://user-images.githubusercontent.com/72603507/168402924-dad985c6-172b-47cb-9a24-b7cb4c69ee39.jpg)

![telegram-cloud-document-2-5474131574771423323](https://user-images.githubusercontent.com/72603507/168402935-769b2dac-5a88-42c8-9f7e-6a567f62b04b.jpg)

![telegram-cloud-document-2-5474131574771423320](https://user-images.githubusercontent.com/72603507/168402942-96fb1be5-a5f4-4bcc-8c2c-2cd80300da53.jpg)

![telegram-cloud-document-2-5474131574771423321](https://user-images.githubusercontent.com/72603507/168402949-513914a4-70c5-43c3-9ac0-7d577e3567d4.jpg)

Тесты:
<img width="1768" alt="image" src="https://user-images.githubusercontent.com/72603507/168401781-1d553d3b-020b-43ec-acda-ecc48fbcaa93.png">

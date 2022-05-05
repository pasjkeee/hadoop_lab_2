#!/bin/bash
if [[ $# -eq 0 ]] ; then
    echo 'You should specify database name!'
    exit 1
fi


export PATH=$PATH:/usr/local/hadoop/bin/
hadoop dfs -rm -r logs
hadoop dfs -rm -r out
# Устанавливаем PostgreSQL
sudo apt-get update -y
sudo apt-get install -y postgresql postgresql-contrib
sudo service postgresql start

# Создаем таблицу с логами
sudo -u postgres psql -c 'ALTER USER postgres PASSWORD '\''1234'\'';'
sudo -u postgres psql -c 'drop database if exists '"$1"';'
sudo -u postgres psql -c 'create database '"$1"';'
sudo -u postgres -H -- psql -d $1 -c 'CREATE TABLE logging (id BIGSERIAL PRIMARY KEY, logLevel int, datetime VARCHAR(20), other VARCHAR(256));'

# Генерируем входные данные и добавляем их в таблицу
POSTFIX=("fccd8a5f3a42,rsyslogd-2007:,action -action 9- suspended, next retry is Fri Oct 26 13:54:37 2018 [v8.16.0 try http://www.rsyslog.com/e/2007 ]"
        "fccd8a5f3a42,rsyslogd:,rsyslogds userid changed to 107")
for i in {1..200}
	do
	    HOUR=$((RANDOM % 24))
	    if [ $HOUR -le 9 ]; then
	        TWO_DIGIT_HOUR="0$HOUR"
	    else
	        TWO_DIGIT_HOUR="$HOUR"
	    fi
		sudo -u postgres -H -- psql -d $1 -c 'INSERT INTO logging (logLevel, datetime, other) values ('"$((RANDOM % 8))"','\''Nov 10 '"$TWO_DIGIT_HOUR"':13:56'\'','\'"${POSTFIX[$((RANDOM % ${#POSTFIX[*]}))]}"''\'');'
	done


# Генерация моя


rm -rf input
mkdir input


UNVIERSITY=("MEPHI" "MADI" "SPBGU")
YEAR=("2019" "2020" "2021")
MONTH=("01" "02" "03" "04" "05" "06" "07" "08" "09" "10" "11" "12")

#"MEPHI;4;26-06-2020 10:11:28;0"

for i in {1..100}
	do
	  UNIVERCITY=${UNVIERSITY[$((RANDOM % ${#UNVIERSITY[*]}))]}
	  ID=$(($RANDOM % 10))
	  HOURSTART=$((10 + $(($RANDOM % 3))))
	  HOUREND=$(($(($RANDOM % 7)) + 1 + $HOURSTART))
	  YEAR=${YEAR[$((RANDOM % ${#YEAR[*]}))]}
	  DAY=$(($i % 10 + 10))
	  MONTH=${MONTH[$((RANDOM % ${#MONTH[*]}))]}

		RESULT1="$UNIVERCITY;$ID;$DAY-$MONTH-$YEAR $HOURSTART:20:20;0"
		RESULT2="$UNIVERCITY;$ID;$DAY-$MONTH-$YEAR $HOUREND:20:20;1"
		echo $RESULT1 >> input/$1.1
		echo $RESULT2 >> input/$1.1
	done


	for i in {1..100}
  	do
  	  UNIVERCITY=${UNVIERSITY[$((RANDOM % ${#UNVIERSITY[*]}))]}
  	  ID=$(($RANDOM % 10))
  	  YEAR=${YEAR[$((RANDOM % ${#YEAR[*]}))]}
  	  DAY=$(($i % 10 + 10))
  	  MONTH=${MONTH[$((RANDOM % ${#MONTH[*]}))]}

  		RESULT="$UNIVERCITY;$ID;NAME_OF_WORK;$DAY-$MONTH-$YEAR"
  		echo $RESULT >> input/$1.1
  	done



# Скачиваем SQOOP
if [ ! -f sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz ]; then
    wget http://apache-mirror.rbc.ru/pub/apache/sqoop/1.4.7/sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz
    tar xvzf sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz
else
    echo "Sqoop already exists, skipping..."
fi

# Скачиваем драйвер PostgreSQL
if [ ! -f postgresql-42.2.5.jar ]; then
    wget --no-check-certificate https://jdbc.postgresql.org/download/postgresql-42.2.5.jar
    cp postgresql-42.2.5.jar sqoop-1.4.7.bin__hadoop-2.6.0/lib/
else
    echo "Postgresql driver already exists, skipping..."
fi

export PATH=$PATH:/sqoop-1.4.7.bin__hadoop-2.6.0/bin

# Скачиваем Spark
if [ ! -f spark-2.3.1-bin-hadoop2.7.tgz ]; then
    wget https://archive.apache.org/dist/spark/spark-2.3.1/spark-2.3.1-bin-hadoop2.7.tgz
    tar xvzf spark-2.3.1-bin-hadoop2.7.tgz
else
    echo "Spark already exists, skipping..."
fi

export SPARK_HOME=/spark-2.3.1-bin-hadoop2.7
export HADOOP_CONF_DIR=$HADOOP_PREFIX/etc/hadoop

sqoop import --connect 'jdbc:postgresql://127.0.0.1:5432/'"$1"'?ssl=false' --username 'postgres' --password '1234' --table 'logging' --target-dir 'logs'

export PATH=$PATH:/spark-2.3.1-bin-hadoop2.7/bin

spark-submit --class bdtc.lab2.SparkSQLApplication --master local --deploy-mode client --executor-memory 1g --name wordcount --conf "spark.app.id=SparkSQLApplication" /tmp/lab2-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs://127.0.0.1:9000/user/root/logs/ out

echo "DONE! RESULT IS: "
hadoop fs -cat  hdfs://127.0.0.1:9000/user/root/out/part-00000





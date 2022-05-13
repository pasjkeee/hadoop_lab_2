Приложение принимает на вход 2 файла:

1) Журнал входа-выхода
<img width="639" alt="image" src="https://user-images.githubusercontent.com/72603507/168401492-e178eb9d-68dd-4518-bbb0-8ad77822ebf2.png">

2) Реестр опубликованных работ
<img width="668" alt="image" src="https://user-images.githubusercontent.com/72603507/168401517-bd0d4f44-fcab-471c-a4b3-a09eb1b96328.png">

Генерация происходит с помошью скрипта

<img width="870" alt="image" src="https://user-images.githubusercontent.com/72603507/168401545-3536563a-6a4b-46c4-8166-e9f0a383b803.png">

Загружаем сгенерированный input в hdfs

```
hdfs dfs -put input
```
Далее эти файлы считываются в программе
<img width="909" alt="image" src="https://user-images.githubusercontent.com/72603507/168401687-22d08e0d-f1ad-4957-8e40-3684277b6c7b.png">

Журнал входа-выхода редьюсится в JavaRDD по шаблону [key: humanId/univercity/year]: spenTimeInYear
Реестр опубликованных работ редьюсится в JavaRDD по шаблону [key: humanId/univercity/year]: numOfWorks

И далее она матчатся по ключу и выводятся в файл


Тесты:
<img width="1768" alt="image" src="https://user-images.githubusercontent.com/72603507/168401781-1d553d3b-020b-43ec-acda-ecc48fbcaa93.png">

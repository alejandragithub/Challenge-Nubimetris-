# Databricks notebook source
from datetime import datetime
from pyspark.sql.functions import flatten

import datetime as dt
import json
import ast
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark
from array import array
import os

# COMMAND ----------

#DESAFIO 3

# COMMAND ----------

dfSellers=spark.read.json("dbfs:/mnt/raw-data/DIP/GDA/SELLERS/")

# COMMAND ----------

dfSellers.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("DB_SELLERS.sellers")

# COMMAND ----------

datos = spark.table("DB_SELLERS.sellers")

# COMMAND ----------

   dfparsed=datos.select(col('body.*'))
   display(dfparsed)

# COMMAND ----------

   dfparsed2=dfparsed.select('site_id','id','nickname','points')
   display(dfparsed2)

# COMMAND ----------

 (dfparsed2
   .withColumnRenamed('id','sellerId').withColumnRenamed('site_id','siteId').withColumnRenamed('nickname','sellerNickname').withColumnRenamed('points','sellerPoints')
        .select('siteId','sellerId','sellerNickname','sellerPoints')
   .write
   .mode("overwrite")
   .option("mergeSchema", "true")
   .format("delta")
   .saveAsTable("DB_SELLERS.sellers")
 )

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions._ /*aquí importamos esta librería para poder especificar el order by con desc. Se puede solo usando limit, pero para más seguridad se agregó la opción desc*/
# MAGIC 
# MAGIC val sqlDFNeg = spark.sql("select siteId,sellerId,sellerNickname,sellerPoints from DB_SELLERS.sellers where sellerPoints < 0")
# MAGIC sqlDFNeg.show()

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.functions._
# MAGIC import java.util.Calendar;
# MAGIC 
# MAGIC 
# MAGIC  sqlDFNeg
# MAGIC       .coalesce(1)
# MAGIC       .write
# MAGIC       .option("header","true")
# MAGIC       .format("com.databricks.spark.csv")
# MAGIC       .option("sep",";")
# MAGIC       .mode("overwrite")
# MAGIC       .save("dbfs:/mnt/raw-data/DIP/GDA/SELLERS/MPE20211010/Negativo/Negativo.csv")
# MAGIC       dbutils.fs.mv(dbutils.fs.ls("dbfs:/mnt/raw-data/DIP/GDA/SELLERS/MPE20211010/Negativo/Negativo.csv").filter(file=>file.name.startsWith("part-00000"))(0).path,"dbfs:/mnt/raw-data/DIP/GDA/SELLERS/MPE20211010/Negativo/Negativo_20211010.csv")
# MAGIC       dbutils.fs.rm("dbfs:/mnt/raw-data/DIP/GDA/SELLERS/MPE20211010/Negativo/Negativo.csv",true)

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions._ /*aquí importamos esta librería para poder especificar el order by con desc. Se puede solo usando limit, pero para más seguridad se agregó la opción desc*/
# MAGIC 
# MAGIC val sqlDFPos = spark.sql("select siteId,sellerId,sellerNickname,sellerPoints from DB_SELLERS.sellers where sellerPoints > 0")
# MAGIC sqlDFPos.show()

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.functions._
# MAGIC import java.util.Calendar;
# MAGIC 
# MAGIC 
# MAGIC  sqlDFPos
# MAGIC       .coalesce(1)
# MAGIC       .write
# MAGIC       .option("header","true")
# MAGIC       .format("com.databricks.spark.csv")
# MAGIC       .option("sep",";")
# MAGIC       .mode("overwrite")
# MAGIC       .save("dbfs:/mnt/raw-data/DIP/GDA/SELLERS/MPE20211010/Positivo/Positivo.csv")
# MAGIC       dbutils.fs.mv(dbutils.fs.ls("dbfs:/mnt/raw-data/DIP/GDA/SELLERS/MPE20211010/Positivo/Positivo.csv").filter(file=>file.name.startsWith("part-00000"))(0).path,"dbfs:/mnt/raw-data/DIP/GDA/SELLERS/MPE20211010/Positivo/Positivo_20211010.csv")
# MAGIC       dbutils.fs.rm("dbfs:/mnt/raw-data/DIP/GDA/SELLERS/MPE20211010/Positivo/Positivo.csv",true)

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions._ /*aquí importamos esta librería para poder especificar el order by con desc. Se puede solo usando limit, pero para más seguridad se agregó la opción desc*/
# MAGIC 
# MAGIC val sqlDFCero = spark.sql("select siteId,sellerId,sellerNickname,sellerPoints from DB_SELLERS.sellers where sellerPoints = 0")
# MAGIC sqlDFCero.show()

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.functions._
# MAGIC import java.util.Calendar;
# MAGIC 
# MAGIC 
# MAGIC  sqlDFCero
# MAGIC       .coalesce(1)
# MAGIC       .write
# MAGIC       .option("header","true")
# MAGIC       .format("com.databricks.spark.csv")
# MAGIC       .option("sep",";")
# MAGIC       .mode("overwrite")
# MAGIC       .save("dbfs:/mnt/raw-data/DIP/GDA/SELLERS/MPE20211010/Cero/Cero.csv")
# MAGIC       dbutils.fs.mv(dbutils.fs.ls("dbfs:/mnt/raw-data/DIP/GDA/SELLERS/MPE20211010/Cero/Cero.csv").filter(file=>file.name.startsWith("part-00000"))(0).path,"dbfs:/mnt/raw-data/DIP/GDA/SELLERS/MPE20211010/Cero/Cero_20211010.csv")
# MAGIC       dbutils.fs.rm("dbfs:/mnt/raw-data/DIP/GDA/SELLERS/MPE20211010/Cero/Cero.csv",true)

# COMMAND ----------

#DESAFIO 4

# COMMAND ----------

# MAGIC %scala
# MAGIC val dfMPE = sqlContext.read.json("dbfs:/mnt/raw-data/DIP/GDA/MPE/")

# COMMAND ----------

dfMPE=spark.read.json("dbfs:/mnt/raw-data/DIP/GDA/MPE/")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS DB_MPE;

# COMMAND ----------

dfMPE.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("DB_MPE.mpe")

# COMMAND ----------

datosmpe = spark.table("DB_MPE.mpe")

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions._
# MAGIC 
# MAGIC  val resu = dfMPE.select(explode($"results")).select( $"col.id", $"col.available_quantity", $"col.sold_quantity")
# MAGIC  resu.show()

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.functions._
# MAGIC import java.util.Calendar;
# MAGIC 
# MAGIC 
# MAGIC  resu
# MAGIC       .coalesce(1)
# MAGIC       .write
# MAGIC       .option("header","true")
# MAGIC       .format("com.databricks.spark.csv")
# MAGIC       .option("sep",";")
# MAGIC       .mode("overwrite")
# MAGIC       .save("dbfs:/mnt/raw-data/DIP/GDA/MPE/MPE.csv")
# MAGIC       dbutils.fs.mv(dbutils.fs.ls("dbfs:/mnt/raw-data/DIP/GDA/MPE/MPE.csv").filter(file=>file.name.startsWith("part-00000"))(0).path,"dbfs:/mnt/raw-data/DIP/GDA/MPE/MPE_20211010.csv")
# MAGIC       dbutils.fs.rm("dbfs:/mnt/raw-data/DIP/GDA/MPE/MPE.csv",true)
# MAGIC       

# COMMAND ----------

# MAGIC 
# MAGIC %scala
# MAGIC resu.createOrReplaceTempView("MPE_temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from MPE_temp

# COMMAND ----------

#DESAFIO 5

# COMMAND ----------

# MAGIC %scala
# MAGIC val dfVisitas= spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("dbfs:/mnt/raw-data/DIP/GDA/MPE/visits.csv")
# MAGIC display(dfVisitas)

# COMMAND ----------

# MAGIC %scala
# MAGIC dfVisitas.createOrReplaceTempView("visitas_temp")

# COMMAND ----------

# MAGIC %scala
# MAGIC val resultado5= sqlContext.sql(
# MAGIC   "select b.itemId, a.sold_quantity, b.visits from MPE_temp a inner join visitas_temp b where a.id = b.itemId")
# MAGIC resultado5.show

# COMMAND ----------

#DESAFIO 6

# COMMAND ----------

# MAGIC %scala
# MAGIC val resultado6= sqlContext.sql("select b.itemId, a.sold_quantity, b.visits, round((a.sold_quantity/b.visits),4) conversionRate, RANK() OVER( ORDER BY round((a.sold_quantity/b.visits),4) DESC ) as conversionRanking from MPE_temp a inner join visitas_temp b where a.id = b.itemId")
# MAGIC resultado6.show

# COMMAND ----------

#DESAFIO 7

# COMMAND ----------

# MAGIC %scala
# MAGIC val resultado7= sqlContext.sql("select id, available_quantity, round((available_quantity*100 / 1421),2) AS stockPercentage from MPE_temp group by id, available_quantity")
# MAGIC resultado7.show

# COMMAND ----------

# MAGIC %scala
# MAGIC val resultado7= sqlContext.sql("select id, available_quantity, round((available_quantity*100 /(select sum(available_quantity) from MPE_temp)),2) AS stockPercentage from MPE_temp group by id, available_quantity")
# MAGIC resultado7.show

# COMMAND ----------

#DESAFIO 8

# COMMAND ----------

import os.path
import os
def generateMonthlyPathList(year, month, day):

  for i in range(1,day+1):
    print(os.path.expandvars('https://importantdata@location/'+str(year)+'/'+ str(month) + '/' + str(i) + '/'))
  return

# COMMAND ----------

resultado8=generateMonthlyPathList(2021, 10, 10)
resultado8

# COMMAND ----------

#DESAFIO 9

# COMMAND ----------

from dateutil.relativedelta import relativedelta
import datetime
import calendar
from datetime import datetime, timedelta
import os.path
import os

def generateLastDaysPaths(date, days):
    end_date = datetime(year=int(date[0:4]), month=int(date[4:6]), day=int(date[6:8]))
    start_date = end_date - timedelta(days=n)
    
  

    day_count = (end_date - start_date).days 
    for single_date in [d for d in (start_date + timedelta(n) for n in range(day_count)) if d <= end_date]:
           #print(os.path.expandvars('https://importantdata@location/'+ str(single_date) + '/'))
           print(os.path.expandvars('https://importantdata@location/'+ str(single_date.strftime("%Y/%m/%d")) + '/'))
    return

# COMMAND ----------

resultado9=generateLastDaysPaths("20211011", 8)
resultado9

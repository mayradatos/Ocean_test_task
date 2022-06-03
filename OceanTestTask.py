#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()
findspark.find()
import pyspark
findspark.find()


# In[2]:


from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
conf = pyspark.SparkConf().setAppName('appName').setMaster('local')
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)


# In[3]:


sc = spark.sparkContext

# using SQLContext to read parquet file
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)


# In[4]:


"""
2)  I decided to use a parquet file because those files are stored like a table (column-oriented format) but as a binary file which helps to save some space in disk.
In addition, parquets files are well integrated with spark, which is capable to undestand and manage its format easily.
"""
df = sqlContext.read.parquet(r'C:\Users\Mayra\Desktop\parquet')


# In[5]:


# A show as a first approach to the information contained in this dataframe. 
#This give me some idea about the type of data that is contained, its format and distribution
df.show(5, False)


# In[6]:


"""
3)
The epochMillis column, contains the details about how many seconds have passed since 1970 January the 1th.
In this case it is a possibility to convert this information to multiple date formats including: datetime and date. 
Doing that i can determine that the period of time contained in the DF is between March 2019 and March 2020.
The dataset contains only information about the last 10 days of March in both years. There is not details about other months in the middle
"""
#

import pyspark.sql.functions as F
from pyspark.sql.functions import col, asc,desc
df.select(F.from_unixtime((df.epochMillis.cast('bigint')/1000)).cast('date').alias('my_date_column')).groupby('my_date_column').count().orderBy(col("count").desc()).show(50)


# In[7]:


#4)
# I have wrote the schema in order to see more details about data types and nested columns
df.printSchema()


# In[8]:


#Doing that, i have discovered that the port column contains names of variables that are being used for other columns, like for example, The latitude and longitude nestedcolumns that are included in position as well 
#Taking that into consideration i have decided to rename those internal columns as p_(nameofthenestcolumn)
from pyspark.sql.functions import *
df.select('epochMillis','mmsi', col("port.unlocode").alias("p_unlocode"),col("port.name").alias("p_name"),col("port.latitude").alias("p_latitude"),col("port.longitude").alias("p_longitude")).show()


# In[9]:


#This step select all the internal columns to be part of the first level of agregation except of the ones that are contained in port variable, which are being renamed one by one to be part of the first level too.
df2= df.select('epochMillis','mmsi', 'position.*','navigation.*','olson_timezone','vesselDetails.*',col("port.unlocode").alias("p_unlocode"),col("port.name").alias("p_name"),col("port.latitude").alias("p_latitude"),col("port.longitude").alias("p_longitude"),'imo','callSign','destination','cargoDetails')


# In[10]:


#The new schema with all the flatten process concluded
df2.printSchema()


# In[11]:


"""
To answer the fourth question, the number of null in each column is counted, All the columns in the dataframe are being written in this output with the detail of number of nulls in each one
Answering the question The top three are: cargoDetails, imo and destination
"""
from pyspark.sql.functions import col,isnan, when, count
df2.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df2.columns]).show()


# In[12]:


#5)
## The df3 was created to contain only the information related with position and coordinates
df3=df2.select('p_unlocode','p_latitude','p_longitude','p_name','latitude','longitude')
df3.show()


# In[13]:


#I have seen that the p_name is exactly the same for the top 20 rows, And i decided to validate how many different names exists in the collection , and i discovered that it is only one.
#This give me the idea that maybe all the coordinates corresponds to the same port, I also noticed that all the port coordinates are exactly the same because they corresponds to the same port.
df3.select('p_name').distinct().show()


# In[14]:


#This query is to validate that the position coordinates are closer to the port coordinates. I decided that two points bigger or smaller are good to do an interesting comparison, If they have a bigger distance maybe we are tolking about other location. 
#This query returns 0 wich means that all the ports informations references the same point.
df3.select('p_latitude','p_longitude','latitude','longitude').filter((col("latitude")+int(2)<col("p_latitude")) | (col("latitude")-int(2)>col("p_latitude")) | (col("longitude")+int(2)<col("p_longitude")) | (col("longitude")-int(2)>col("p_longitude"))).count()


# In[15]:


#with 1.9 distance (which is relatively small distance), i found 421579 rows that represents the 12% of the sample.
#In conclusion I think that aproximately 88% of the data comes from the same port for sure, and the rest of the rows has a distance that is too small. 

df3.select('p_latitude','p_longitude','latitude','longitude').filter((col("latitude")+int(1.9)<col("p_latitude")) | (col("latitude")-int(1.9)>col("p_latitude")) | (col("longitude")+int(1.9)<col("p_longitude")) | (col("longitude")-int(1.9)>col("p_longitude"))).count()


# In[16]:


df3.count()


# In[17]:


#I also validated the destinations, but there are to many different values and reading the rows it does not look to clean to provide extra information easily. I would like to search for destination strings that are too similar one with the other to provide extra information about destination details
df2.select('destination').distinct().count()


# In[18]:


#R: This data represents information about the SHANGHAI port
df2.select('destination').distinct().show(1000)


# In[19]:


#6)
#this show is to see information about the fields that are being asked to calculate a frequency tabulation. I can see here, that are related in its values and the navCode references one navDesc. At least in the sample
#That give me the idea to evaluate those two together to generate the proper groups
df2.select("navCode","navDesc").show(30,False)


# In[20]:


#This is the frequency tabulation ordered by number of occurrencies in desc order. 
df2.groupBy("navCode","navDesc").count().orderBy(col("count").desc()).show(50)


# In[21]:


#For mi is interesting to see how many timezones the dataset contains. In this moment i was assuming that only one because of the ports coordinates. But i was not sure.
#This is interesting to me, because if a data set is containing more than one time zone. Is a good idea to have an extra column with all the information related with the time but in the same time zone, This makes easier some data evaluations and comparisons 
df2.select("olson_timezone").distinct().show()


# In[22]:


# I have seen that the dataset contains different types of vessels and for me is interesting to count how much of each type, Which types are the most populars, How many types exists, etc
df2.groupBy("typeName").count().orderBy(col("count").desc()).show(50)


# In[23]:


#I also noticed that the vessels are for different countries and this visualization is to see the distribution of these countries, the most representatives.
#This reinforce my response to 5th question because the majority of the vessels are from China.
df2.groupBy("flagCountry").count().orderBy(col("count").desc()).show(50)


# In[24]:


#I have taken the top ten countries according to the last result.
countries=["China","Panama","Hong Kong","Liberia","Marshall Islands","Singapore","Malta","South Korea","Madeira","Belize"]
df4=df2.filter(df2.flagCountry.isin(countries))


# In[25]:


#This is the count for only those countries which compared with the original one is more than the 97% of the rows
df4.count()


# In[26]:


#this is a cross table between the country and the type of vessels. This can give information about which type of vessels some countries preferes.
#The influence of each country in the vesselss types. If maybe only one country has one specific type of vessels, The most popular vessels accross countries, etc
df4.crosstab('typeName', 'flagCountry').show(200,False)


# In[28]:


#the nabcodes presence in each of the most relevant countries in the dataset
df4.crosstab('navCode', 'flagCountry').show(200,False)


# In[29]:


#The vessels speed compared with the type of vessels, to see if they are consistent or start to imaginate the gauss bell of speeds for each type
df2.crosstab('speedOverGround', 'typeName').show(200,False)


# In[30]:


"""
7) 
The top 5 navCodes are the following, I am doing a filter in the dataframe to get only the rows that has one of this navcodes
""" 
navcodes=[16,0,5,1,15]
df5=df2.filter(df2.navCode.isin(navcodes))


# In[31]:


#count of this subset of data
df5.count()


# In[32]:


#to prepare this subset i am only recovering the columns that the excercise ask for, also i am transforming the epoch to timestamp
df6=df5.select('navCode','navDesc','mmsi',F.from_unixtime((df.epochMillis.cast('bigint')/1000)).cast('timestamp').alias('timestamp')).where(col("mmsi")=="205792000").orderBy("timestamp")


# In[33]:


df6.show(500,False)


# In[34]:


#This is a window function that orders the data by timestamp and navcode. 
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
windowSpec  = Window.orderBy("timestamp","navCode")


# In[35]:


#taking this window function into consideration, i am calculating here a flag which represents the break point between one collection and the next one assigning to each collection a unique number, The first row of each "group" will have a null value  
from pyspark.sql.functions import monotonically_increasing_id,row_number
df7=df6.withColumn('flag', when(lag(col("navDesc"),1).over(windowSpec) != col('navDesc'), monotonically_increasing_id()).otherwise(None)) .withColumn('flag', coalesce(col("flag"), last(col("flag"), True).over(Window.orderBy("timestamp"))))


# In[36]:


"""Now, all the elements that are part of the same collection had been categorized by the flag column, that is because the second window is partitioning by flag. The order by timestamp is required to mantain the groups ordered
With this window created it is posible to evaluate each partition individually. The prev_timestamp it the value in the timestamp in the previous row
and the difference in seconds is the substraction between those two dates. 
sum_secs is a column to sum up all the sec_differences for each partition.
Finally, to return only the last element of this partition i am creating an extraflag that differences those rows to the rest of them.
"""
windowSpec  = Window.partitionBy("flag").orderBy("timestamp")
df8= df7.withColumn('prev_timestamp', lag(col("timestamp"),1).over(windowSpec)) .withColumn('sec_difference',col("timestamp").cast("long") - col("prev_timestamp").cast("long")).withColumn('sum_secs',sum(col("sec_difference")).over(windowSpec)).withColumn('last',when(lead(col("prev_timestamp"),1).over(windowSpec).isNull(), lit(1)).otherwise(lit(0)))


# In[37]:


#Finally i am filtering the last row of each partition
df8.select("navCode","navDesc","mmsi","timestamp","sum_secs").where(col("last")==1).show()


# In[38]:


#This is the partitioned table with all the columns as reference
df8.show(200,False)


# In[40]:


"""
8) 
The top 5 navCodes, and the nwe mnsi
""" 
df9=df5.select('navCode','navDesc','mmsi',F.from_unixtime((df.epochMillis.cast('bigint')/1000)).cast('timestamp').alias('timestamp')).where(col("mmsi")=="413970021").orderBy("timestamp")


# In[41]:


from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
windowSpec  = Window.orderBy("timestamp","navCode")
df10=df9.withColumn('flag', when(lag(col("navDesc"),1).over(windowSpec) != col('navDesc'), monotonically_increasing_id()).otherwise(None)) .withColumn('flag', coalesce(col("flag"), last(col("flag"), True).over(Window.orderBy("timestamp"))))


# In[42]:


"""All the elements that are part of the same collection had been categorized by the flag column, that is because the second window is partitioning by flag. The order by timestamp is required to mantain the groups ordered
With this window created it is posible to evaluate each partition individually. The prev_timestamp it the value in the timestamp in the previous row
and the difference in seconds is the substraction between those two dates. 
sum_secs is a column to sum up all the sec_differences for each partition.
Finally, to return only the last element of this partition i am creating an extraflag that differences those rows to the rest of them.
"""
windowSpec  = Window.partitionBy("flag").orderBy("timestamp")
df11= df10.withColumn('prev_timestamp', lag(col("timestamp"),1).over(windowSpec)) .withColumn('sec_difference',col("timestamp").cast("long") - col("prev_timestamp").cast("long")).withColumn('sum_secs',sum(col("sec_difference")).over(windowSpec)).withColumn('last',when(lead(col("prev_timestamp"),1).over(windowSpec).isNull(), lit(1)).otherwise(lit(0)))


# In[43]:


df11.show(200,False)


# In[44]:


df11.select("navCode","navDesc","mmsi","timestamp","sum_secs").where(col("last")==1).show()


# In[47]:


df2.select("mmsi").distinct().count()


# In[46]:


df2.select("mmsi").distinct().where(col("navCode")==16).count()


# In[48]:


"""The most popular nav is the Unknown one with in addition has 16462 differents vessels, 
The navcode is consistent, for all the unknown navs, 
In the previous window i can see a time between one row and the following one is arroud ten minutes but for this nav, the time separation is shorter and some times nonexistent like in this example: 
|16     |Unknown|413970021|2019-03-22 18:22:14|null|2019-03-22 18:22:14|0             |1940    |0   |
|16     |Unknown|413970021|2019-03-22 18:31:58|null|2019-03-22 18:22:14|584           |2524    |0   |
This, taking in consideration only this mmsi, but this nav is serving some of them at the time
This makes me think that it is a generic value, Which means that it like the default value for the nav  that had not been registered previously in the system 
in short, For me the Unknown nav refers to a group to a non specific navs (unlike a specific one).
"""


# In[ ]:





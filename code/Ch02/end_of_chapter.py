# end-of-chapter.py############################################################
#
# Use this to get a free pass from Chapter 2 to Chapter 3.
#
# Remember, with great power comes great responsibility. Make sure you
# understand the code before running it! If necessary, refer to the text in
# Chapter 2.
#
###############################################################################

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, lower, regexp_extract
spark = SparkSession.builder.getOrCreate()

book = spark.read.text("../../data/gutenberg_books/1342-0.txt")
lines = book.select(split(book.value, " ").alias("line"))
words = lines.select(explode(col("line")).alias("word"))
words_lower = words.select(lower(col("word")).alias("word_lower"))
words_clean = words_lower.select(regexp_extract(col("word_lower"), "[a-z]*", 0).alias("word"))

words_nonull = words_clean.where(col("word") != "")



# end-of-chapter.py############################################################
# JOELS VERSION 2024.11.06
#
###############################################################################
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, lower, regexp_extract

# Download the file
url = "https://raw.githubusercontent.com/Huitzilopoctli372/DataAnalysisWithPythonAndPySpark/trunk/data/1342-0.txt"
response = requests.get(url)
with open("1342-0.txt", "wb") as f:
    f.write(response.content)

# Initiate a spark session and create a spark of object Sparksession type
spark = SparkSession.builder.getOrCreate()

# Read data into a dataframe
book = spark.read.csv("1342-0.txt", sep='\n', header=False).withColumnRenamed("_c0", "value")

# Clean/Transform data
lines = book.select(split(book.value, " ").alias("line"))
words = lines.select(explode(col("line")).alias("word"))
words_lower = words.select(lower(col("word")).alias("word_lower"))
words_clean = words_lower.select(regexp_extract(col("word_lower"), "[a-z]*", 0).alias("word"))
words_nonull = words_clean.where(col("word") != "")

# Show
words_nonull.show()

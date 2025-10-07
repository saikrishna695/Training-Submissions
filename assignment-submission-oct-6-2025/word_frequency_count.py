import os
os.environ["PYSPARK_PYTHON"] = r"C:\Users\asus\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\asus\AppData\Local\Programs\Python\Python311\python.exe"

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WordCount").getOrCreate()

text_file = spark.sparkContext.textFile(r"D:\Training-Submissions\assignment-submission-oct-6-2025\countdown.txt")

words = text_file.flatMap(lambda line: line.split(" "))
word_pairs = words.map(lambda word: (word.lower(), 1))
word_counts = word_pairs.reduceByKey(lambda a, b: a + b)

for word, count in word_counts.collect():
    print(f"{word}: {count}")

spark.stop()

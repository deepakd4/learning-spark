from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
textFile = spark.read.text("C:\Softwares\spark-3.0.1-bin-hadoop2.7\README.md")

num_of_As = textFile.filter(textFile.value.contains('a')).count()
num_of_Bs = textFile.filter(textFile.value.contains('b')).count()

print("Lines with a : %i, lines with b : %i" % (num_of_As, num_of_Bs))

spark.stop()


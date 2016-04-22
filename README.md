# probable-carnival

spark-submit --class Kmeans --master local[2] --jars $(find /root/JCuda-All-0.7.5-bin-Linux-x86_64/| grep '\.jar'|tr '\n' ',')  target/scala-2.10/kmeans_2.10-1.0.jar

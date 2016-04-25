./sbt/sbt package
nvcc -m64 -ptx cuda_kmeans.cu -o cuda_kmeans.ptx
~/spark/bin/spark-submit --class Kmeans --master local[2] --jars $(find /root/JCuda-All-0.7.5-bin-Linux-x86_64/| grep '\.jar'|tr '\n' ',')  target/scala-2.10/kmeans_2.10-1.0.jar train /root/input-tweets.txt /root/probable-carnival/out 10 0.0001 20
#~/spark/bin/spark-submit --class Kmeans --master local[2] --jars $(find /root/JCuda-All-0.7.5-bin-Linux-x86_64/| grep '\.jar'|tr '\n' ',')  target/scala-2.10/kmeans_2.10-1.0.jar predict /root/input-tweets.txt /root/probable-carnival/out.cluster_centres /root/probable-carnival/predict.out


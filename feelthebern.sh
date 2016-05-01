#./sbt/sbt package
#nvcc -m64 -ptx cuda_kmeans.cu -o cuda_kmeans.ptx
#~/spark/bin/spark-submit --class Kmeans --master spark://ec2-54-186-103-10.us-west-2.compute.amazonaws.com:7077 --jars $(find /root/JCuda-All-0.7.5-bin-Linux-x86_64/| grep '\.jar'|tr '\n' ',')  target/scala-2.10/kmeans_2.10-1.0.jar train /root/input-tweets.txt /root/probable-carnival/out 10 0.0001 20

#~/spark/bin/spark-submit --driver-memory 10g --class Kmeans --jars $(find /root/JCuda-All-0.7.5-bin-Linux-x86_64/| grep '\.jar'|tr '\n' ',')  target/scala-2.10/kmeans_2.10-1.0.jar train /root/input-tweets.txt /root/probable-carnival/out 7 0.0001 1000
#~/spark/bin/spark-submit --class Kmeans --jars $(find /root/JCuda-All-0.7.5-bin-Linux-x86_64/| grep '\.jar'|tr '\n' ',')  target/scala-2.10/kmeans_2.10-1.0.jar train /root/kmeans/Image_data/color100.txt /root/probable-carnival/out 7 0.0001 1000
#~/spark/bin/spark-submit --class kmeans.Kmeans --master local[2] --jars $(find /root/JCuda-All-0.7.5-bin-Linux-x86_64/| grep '\.jar'|tr '\n' ',')  target/scala-2.10/kmeans_2.10-1.0.jar par_train file:///root/kmeans/Image_data/color100.txt /root/probable-carnival/out 7 0.0001 20
~/spark/bin/spark-submit --class kmeans.Kmeans --master local[2] --jars $(find /root/JCuda-All-0.7.5-bin-Linux-x86_64/| grep '\.jar'|tr '\n' ',')  target/scala-2.10/kmeans_2.10-1.0.jar par_train file:///root/input-tweets.txt /root/probable-carnival/out 7 0.0001 20
# ~/spark/bin/spark-submit --class Kmeans --master spark://ec2-54-186-103-10.us-west-2.compute.amazonaws.com:7077 --jars $(find /root/JCuda-All-0.7.5-bin-Linux-x86_64/| grep '\.jar'|tr '\n' ',')  target/scala-2.10/kmeans_2.10-1.0.jar predict /root/input-tweets.txt /root/probable-carnival/out.cluster_centres /root/probable-carnival/predicted_out



ARRAY=(ec2-54-191-193-2.us-west-2.compute.amazonaws.com 
ec2-54-191-231-239.us-west-2.compute.amazonaws.com 
ec2-54-187-151-127.us-west-2.compute.amazonaws.com 
ec2-54-186-101-183.us-west-2.compute.amazonaws.com)

echo ${ARRAY[*]}
for i in ${ARRAY[@]}; do
    echo $i  # (i.e. do action / processing of $databaseName here...)
    #ssh -t $i './install.sh' 
    #scp /root/input-tweets.txt $i:~
    ssh -t $i 'cp /root/JCuda-All-0.7.5-bin-Linux-x86_64/*.so /lib'
done


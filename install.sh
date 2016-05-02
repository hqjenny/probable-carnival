sudo yum groupinstall -y "Development tools"
sudo yum install kernel-devel-`uname -r`
wget http://us.download.nvidia.com/XFree86/Linux-x86_64/361.42/NVIDIA-Linux-x86_64-361.42.run
sudo /bin/bash ./NVIDIA-Linux-x86_64-361.42.run

wget http://developer.download.nvidia.com/compute/cuda/7.5/Prod/local_installers/cuda-repo-rhel6-7-5-local-7.5-18.x86_64.rpm
sudo rpm -i cuda-repo-rhel6-7-5-local-7.5-18.x86_64.rpm
rm cuda-repo-rhel6-7-5-local-7.5-18.x86_64.rpm
sudo yum groupinstall ‘Development Tools’ ‘Development Libraries’
yum install nvcc cuda-toolkit*
wget http://www.jcuda.org/downloads/JCuda-All-0.7.5-bin-linux-x86_64.zip
unzip JCuda-All-0.7.5-bin-linux-x86_64.zip
cp /root/JCuda-All-0.7.5-bin-Linux-x86_64/*.so /lib
rm JCuda-All-0.7.5-bin-linux-x86_64.zip

echo export PATH=/usr/local/cuda-7.5/bin:$PATH >> ~/.bash_profile
echo export PATH=/root/spark/bin:$PATH  >> ~/.bash_profile
echo export LD_LIBRARY_PATH=/usr/local/cuda-7.5/lib64:$LD_LIBRARY_PATH >> ~/.bash_profile
echo export LD_LIBRARY_PATH=/root/JCuda-All-0.7.5-bin-Linux-x86_64:$LD_LIBRARY_PATH >> ~/.bash_profile


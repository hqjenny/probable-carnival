package kmeans
import org.apache.spark.SparkFiles
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext._
//remove if not needed
import scala.collection.JavaConversions._

import jcuda.driver.JCudaDriver._
import java.io._
import jcuda._
import jcuda.driver._
import jcuda.runtime._

object Training{

  //def cuda_kmeans_master(sc: SparkContext, objects: Array[Array[Float]], numCoords: Int, numObjs: Int, numClusters: Int, threshold: Float, loop_iterations: Int): (Array[Float], Array[Int]) = {
  def cuda_kmeans_master(sc: SparkContext, filename: String, numClusters: Int, threshold: Float, loop_iterations: Int): (Array[Float], Array[Int], Int, Int) = {
    val lines = sc.textFile(filename, 32)
    val floatRDD = lines.map(x => x.trim.split(' ').slice(1, x.length).map(x => x.trim.toFloat))
    floatRDD.cache()

    val numObjs = floatRDD.count().toInt
    val clusterLines = floatRDD.take(numClusters)
    val startObjects  = clusterLines.map(x => x.trim).map(x => x.split(' ').slice(1, x.length).map(x => x.trim.toFloat))
    val numCoords = startObjects(0).length
    println("numObjs " + numObjs + " numCoords " + numCoords)

    var i, j, index, loop = 0 
    var newClusterSize = Array.fill(numClusters){0} // number of objs assigned in each cluster
    var delta = 0.0f
    // [numCoords][numObjs] 
    //val dimObjects = Array.ofDim[Float](numCoords, numObjs)
    //val dimObjects = Array.ofDim[Float](numCoords * numObjs)
    // [numClusters][numCoords] 
    val clusters= Array.ofDim[Float](numClusters * numCoords)
    // [numCoords][numClusters]
    val dimClusters = Array.ofDim[Float](numCoords * numClusters)
    var newClusters = Array.fill(numCoords * numClusters){0.0f}
    var membership = Array.fill(numObjs){-1}

    /* initialize */
    /*for (i <- 0 until numCoords) {
      for (j <- 0 until numObjs) {
        //dimObjects(Kmeans.get_index(i, j, numObjs)) = objects(Kmeans.get_index(j, i, numCoords)) 
        dimObjects(i)(j) = objects(j)(i) 
      }
    }*/
    for (i <- 0 until numCoords) {
      for (j <- 0 until numClusters) {
        //dimClusters(Kmeans.get_index(i, j, numClusters)) = dimObjects(Kmeans.get_index(i, j, numObjs)) 
        dimClusters(Kmeans.get_index(i, j, numClusters)) = startObjects(j)(i)
      }
    }

    // It will be nice if it can run on transpose rdd but the membership info will be incorrect
    //val objects_RDD = sc.parallelize(dimObjects) // run on 1 partition. RDD[(Float,String)]
    //val objects_RDD = sc.parallelize(objects) // run on 1 partition. RDD[(Float,String)]
    // Get the number of objects in each partition 
    //val partition_size =objects_RDD.mapPartitions(iter => Array(iter.size).iterator, true).collect()
    val partition_size = floatRDD.mapPartitions(iter => Array(iter.size).iterator, true).collect()
    // Get a accumulative sum
    val partition_index = partition_size.scanLeft(0)(_+_)

    // println("partition_index:")
    // partition_index.map(x=> {println(x)})
    // println()
    
    val timestamp1: Long = System.currentTimeMillis 
    do {
      println("loop_iterations: " + loop )
      // println("membership length "+membership.length + " numObjs " + numObjs)

      val model_RDD = floatRDD.mapPartitionsWithIndex((i, t) => { 
      //val model_RDD = objects_RDD.mapPartitionsWithIndex((i, t) => { 
          // println("i " + i +  " start " + partition_index(i) +" end " + partition_index(i+1) + " memlength " + membership.slice(partition_index(i), partition_index(i+1)).length + " memtotallength " + membership.length ); 
        cuda_kmeans_slaves(t, dimClusters, numCoords, numClusters, threshold, membership.slice(partition_index(i), partition_index(i+1)), loop_iterations)}) //map()  // mapPartitions(Iterator[T]) 
      val model_arr = model_RDD.collect()  // Array of Models
      
         // Reset all the values
      delta = 0
      newClusters.map(_=>0.0f)
      newClusterSize.map(_=>0)
      membership = Array()

      for (i <- 0 until model_arr.length){
        if (i % 4 == 0) {
          val newClusters_part = model_arr(i).asInstanceOf[Array[Float]]
          newClusters = (newClusters_part, newClusters).zipped.map(_+_)
        }else if (i % 4 == 1){
          val newClusterSize_part = model_arr(i).asInstanceOf[Array[Int]]
          // println("model " + i + " clustersize " + newClusterSize_part.length)
          newClusterSize = (newClusterSize_part, newClusterSize).zipped.map(_+_) 
        }else if (i % 4 == 2){
          // JENNY need to think a way to pass it 
          membership = membership ++ model_arr(i).asInstanceOf[Array[Int]]
          // println("model " + i + " membership ")
          // membership.map(x=>print(" " + x + " "))
          // println()
        }else{
          delta += model_arr(i).asInstanceOf[Float]
       }
      }

        //  TODO: Change layout of newClusters to [numClusters][numCoords]
        /* average the sum and replace old cluster centers with newClusters */
        for (i <- 0 until numClusters) {
          for (j <- 0 until numCoords) {
            if (newClusterSize(i) > 0) {
              dimClusters(Kmeans.get_index(j, i, numClusters)) = newClusters(Kmeans.get_index(j, i, numClusters)) / newClusterSize(i)
            }
            newClusters(Kmeans.get_index(j, i, numClusters)) = 0.0f 
          }
          newClusterSize(i) = 0
        }

        delta = delta / numObjs
        loop += 1
      } while (delta > threshold && loop < loop_iterations)

    val timestamp2: Long = System.currentTimeMillis 
    val train_time  = (timestamp2 - timestamp1)

    println("\tloop_iterations: " + loop )
    println("\tdelta: " + delta )
    println("\ttrain: " + train_time + "ms" )

    for (i <- 0 until numClusters) {
      for (j <- 0 until numCoords) {
              clusters(Kmeans.get_index(i, j, numCoords)) = dimClusters(Kmeans.get_index(j, i, numClusters))
      }
    }
    return (clusters, membership, numObjs, numCoords)
  }

  // out: [numClusters][numCoords]
  // objects: [numObjs][numCoords]
  def cuda_kmeans_slaves(t: Iterator[Array[Float]], dimClusters: Array[Float], numCoords: Int, numClusters: Int, threshold: Float, membership: Array[Int], loop_iterations: Int):Iterator[Any] = {
    // JENNY: should change the objects to 2D array and pass 1 pointers but they won't be continous; later  
    // Arrays of strings 

    val timestamp1: Long = System.currentTimeMillis
    // val objects_arr = t.toArray
    // val numObjs = objects_arr.length
    //val objects = objects_arr.flatMap(x => x.trim).map(x => x.split(' ').slice(1, x.length).map(x => x.trim.toFloat))
    // val objects = objects_arr.flatMap(x => x.trim.split(' ').slice(1, x.length).map(x => x.trim.toFloat))
    //val objects = t.toArray
    //val numObjs = objects_arr.length
    //val objects = objects_arr.reduce(_++_)
    val objects_arr = t.toArray
    val numObjs = objects_arr.length
    val objects = objects_arr.reduce(_++_)
    
    val timestamp2: Long = System.currentTimeMillis
    val dimObjects = Array.ofDim[Float](numCoords * numObjs)
    /* initialize */
    for (i <- 0 until numCoords) {
      for (j <- 0 until numObjs) {
        dimObjects(Kmeans.get_index(i, j, numObjs)) = objects(Kmeans.get_index(j, i, numCoords)) 
      }
    }

    val timestamp3: Long = System.currentTimeMillis
    JCudaDriver.setExceptionsEnabled(true)
    val ptxFileName = SparkFiles.get("cuda_kmeans.ptx")
    cuInit(0)
    val device = new CUdevice()
    cuDeviceGet(device, 0)
    val context = new CUcontext()
    cuCtxCreate(context, 0, device)
    val module = new CUmodule()
    cuModuleLoad(module, ptxFileName)
    val function1 = new CUfunction()
    //cuModuleGetFunction(function1, module, "find_nearest_cluster")
    cuModuleGetFunction(function1, module, "_Z20find_nearest_clusteriiiPfS_PiS0_")
    val function2 = new CUfunction()
    //cuModuleGetFunction(function2, module, "compute_delta")
    cuModuleGetFunction(function2, module, "_Z13compute_deltaPiii")

    var i, j, loop = 0 
    val newClusterSize = Array.fill(numClusters){0} // number of objs assigned in each cluster
    var delta = 0.0f
    // [numCoords][numObjs] 
    // [numClusters][numCoords] 
    //val clusters= Array.ofDim[Float](numClusters * numCoords)
    // [numCoords][numClusters]
    //val dimClusters = Array.ofDim[Float](numCoords * numClusters)
    val newClusters = Array.fill(numCoords * numClusters){0.0f}

    val deviceObjects = new CUdeviceptr()
    val deviceClusters = new CUdeviceptr()
    val deviceMembership = new CUdeviceptr()
    val deviceIntermediates = new CUdeviceptr()

    //  To support reduction, numThreadsPerClusterBlock *must* be a power of
    //  two, and it *must* be no larger than the number of bits that will
    //  fit into an unsigned char, the type used to keep track of membership
    //  changes in the kernel.
    val numThreadsPerClusterBlock = 128
    val numClusterBlocks = (numObjs + numThreadsPerClusterBlock - 1) / numThreadsPerClusterBlock;

    //#if BLOCK_SHARED_MEM_OPTIMIZATION
    // val clusterBlockSharedDataSize = numThreadsPerClusterBlock * Sizeof.CHAR + numClusters * numCoords * Sizeof.FLOAT
    
    // val deviceNum = Array(-1)
    // JCuda.cudaGetDevice(deviceNum)
    // val deviceProp = new cudaDeviceProp()
    // JCuda.cudaGetDeviceProperties(deviceProp, deviceNum(0))
    // if(clusterBlockSharedDataSize > deviceProp.sharedMemPerBlock) {
    //   println("WARNING: Your CUDA hardware has insufficient block shared memory.")
    //   println("You need to recompile with BLOCK_SHARED_MEM_OPTIMIZATION=0. ")
    //   println("See the README for details.")
    // }

    //#else
    val clusterBlockSharedDataSize = numThreadsPerClusterBlock * Sizeof.CHAR;
    // #endif 

    val numReductionThreads = Kmeans.nextPowerOfTwo(numClusterBlocks)
    val reductionBlockSharedDataSize = numReductionThreads * Sizeof.INT

    cuMemAlloc(deviceObjects, numCoords * numObjs * Sizeof.FLOAT)
    cuMemAlloc(deviceClusters, numCoords * numClusters * Sizeof.FLOAT)
    cuMemAlloc(deviceMembership, numObjs * Sizeof.INT)
    cuMemAlloc(deviceIntermediates, numReductionThreads * Sizeof.INT) // unsigned data type


    cuMemcpyHtoD(deviceObjects, Pointer.to(dimObjects), numObjs * numCoords * Sizeof.FLOAT)
    cuMemcpyHtoD(deviceMembership, Pointer.to(membership), numObjs * Sizeof.INT)

    val timestamp4: Long = System.currentTimeMillis


    //do {

    val timestamp5: Long = System.currentTimeMillis 
        cuMemcpyHtoD(deviceClusters, Pointer.to(dimClusters), numClusters * numCoords * Sizeof.FLOAT)

        val kernelParameters1 = Pointer.to(Pointer.to(Array(numCoords)), Pointer.to(Array(numObjs)), Pointer.to(Array(numClusters)),  Pointer.to(deviceObjects), Pointer.to(deviceClusters), Pointer.to(deviceMembership), Pointer.to(deviceIntermediates))

        // <<< numClusterBlocks, numThreadsPerClusterBlock, clusterBlockSharedDataSize >>>
        cuLaunchKernel(function1, numClusterBlocks, 1, 1, numThreadsPerClusterBlock, 1, 1, clusterBlockSharedDataSize, null, kernelParameters1, null)

        JCuda.cudaDeviceSynchronize()
        var e = JCuda.cudaGetLastError()
        if(e !=  cudaError.cudaSuccess) {
          printf("CUDA Error %d: %s\n", e, JCuda.cudaGetErrorString(e))
        }

        // (deviceIntermediates, numClusterBlocks, numReductionThreads);
        val kernelParameters2 = Pointer.to(Pointer.to(deviceIntermediates), Pointer.to(Array(numClusterBlocks)), Pointer.to(Array(numReductionThreads)))

        // compute_delta <<< 1, numReductionThreads, reductionBlockSharedDataSize >>>
        cuLaunchKernel(function2, 1, 1, 1, numReductionThreads, 1, 1, reductionBlockSharedDataSize, null, kernelParameters2, null) 

        JCuda.cudaDeviceSynchronize()
        e = JCuda.cudaGetLastError()
        if(e !=  cudaError.cudaSuccess) {
          printf("CUDA Error %d: %s\n", e, JCuda.cudaGetErrorString(e))
        }
 
        val d = Array(0)
        cuMemcpyDtoH(Pointer.to(d), deviceIntermediates, Sizeof.INT )
        delta = d(0).toFloat
        cuMemcpyDtoH(Pointer.to(membership), deviceMembership, numObjs * Sizeof.INT)
        // println("membership size " + membership.length + "numObjs " + numObjs)
        for (i <- 0 until numObjs) {
          var index = membership(i)
          newClusterSize(index) += 1;
          for (j <- 0 until numCoords){
            //printf("i %d j %d index %d", i, j, index)
            //printf("newCluster %d idx %d", newClusters.length, Kmeans.get_index(j, index, numClusters))
            //printf("objects %d idx %d\n", objects.length, Kmeans.get_index(j, index, numObjs))
            //newClusters(Kmeans.get_index(j, index, numClusters)) += objects(Kmeans.get_index(i, j, numCoords))
            newClusters(Kmeans.get_index(j, index, numClusters)) += dimObjects(Kmeans.get_index(j, i, numObjs))
          }
        }
        // Uncomment this to allow mulitple iterations on GPU 
        /* average the sum and replace old cluster centers with newClusters */
        /*for (i <- 0 until numClusters) {
          for (j <- 0 until numCoords) {
            if (newClusterSize(i) > 0) {
              dimClusters(Kmeans.get_index(j, i, numClusters)) = newClusters(Kmeans.get_index(j, i, numClusters)) / newClusterSize(i)
            }
            newClusters(Kmeans.get_index(j, i, numClusters)) = 0.0f 
          }
          newClusterSize(i) = 0
        }
    
        delta = delta / numObjs
        loop += 1
      } while (delta > threshold && loop < loop_iterations)
      */ 
    val timestamp6: Long = System.currentTimeMillis 

    val datatime = timestamp2 - timestamp1
    val inversetime = timestamp3 - timestamp2
    val copytime = timestamp4 - timestamp3
    val train_time  = (timestamp6 - timestamp5)

    println("\tWithin cuda_kmeans jobs")
    println("\t\tData conversion time" + datatime + "ms")
    println("\t\tData inverse time" + inversetime + "ms")
    println("\t\tCopying to CUDA time" + copytime + "ms")
    println("\t\tTraining time: " + train_time + "ms" )
    //println("\tloop_iterations: " + loop )
    println("\t\tdelta: " + delta )
    cuMemFree(deviceObjects)
    cuMemFree(deviceClusters)
    cuMemFree(deviceMembership)
    cuMemFree(deviceIntermediates)
    
    cuCtxDestroy(context)
    return (newClusters, newClusterSize, membership, delta).productIterator 
  }



}

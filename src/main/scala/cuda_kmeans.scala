package kmeans
import jcuda.driver.JCudaDriver._
import java.io._
import jcuda._
import jcuda.driver._
import jcuda.runtime._

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


object Kmeans{

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("dataframe kmeans gpu")
    val sc = new SparkContext(conf)
    val sqlContext= new SQLContext(sc)

    import sqlContext.implicits._ 

    if (args.length < 4) {
      System.err.println("Usage: " + this.getClass.getSimpleName +
        "train <input_file> <output_file> <numClusters> <threshold> <iteration>")
      System.err.println("Usage: " + this.getClass.getSimpleName +
        "predict <input_file> <cluster_file> <output_file>")
      System.exit(1)
    }

    val m = args(0)
    if (m == "predict") {
      val Array( mode, input_file, cluster_file, output_file) = args
      sc.addFile("cuda_kmeans.ptx")
      predict(input_file, cluster_file, output_file)

    } else if (m == "par_train") {
      val Array( mode, input_file, output_file, numClusters_str, threshold_str, iterations_str) = args
      val numObjs = Array(0) 
      val numCoords = Array(0) 
      val numClusters = numClusters_str.toInt
      val threshold = threshold_str.toFloat
      val iterations = iterations_str.toInt
      // Add the file to the slave 
      sc.addFile("cuda_kmeans.ptx")

      // Read input file on master
      val timestamp0: Long = System.currentTimeMillis  
      //val objects = read_file_2D(input_file, numObjs, numCoords)
      val timestamp1: Long = System.currentTimeMillis 
      
      //val ret = Training.cuda_kmeans_master(sc, objects, numCoords(0), numObjs(0), numClusters, threshold, iterations)
      val ret = Training.cuda_kmeans_master(sc, input_file, numClusters, threshold, iterations)
      val clusters = ret._1
      val membership = ret._2
      numObjs(0) = ret._3
      numCoords(0) = ret._4
      //def cuda_kmeans_master(sc: SparkContext, objects: Array[Array[Float]], numCoords: Int, numObjs: Int, numClusters: Int, threshold: Float, loop_iterations: Int): Array[Float] = { 
      val timestamp2: Long = System.currentTimeMillis 
      write_file_hdfs(output_file, numClusters, numObjs(0), numCoords(0), clusters, membership)
      val timestamp3: Long = System.currentTimeMillis 

      val read_file_time = (timestamp1 - timestamp0)
      val cuda_kmeans_time  = (timestamp2 - timestamp1)
      val write_file_time  = (timestamp3 - timestamp2)

      println("read_file: " + read_file_time + "ms")
      println("cuda_kmeans_time: " + cuda_kmeans_time + "ms")
      println("write_file_time: " + write_file_time + "ms") 
    } else {
      val Array( mode, input_file, output_file, numClusters_str, threshold_str, iterations_str) = args

      // Number obtained from the input file 
      val numObjs = Array(0) 
      val numCoords = Array(0) 
      val numClusters = numClusters_str.toInt
      val threshold = threshold_str.toFloat
      val iterations = iterations_str.toInt

      // Add the file to the slave 
      sc.addFile("cuda_kmeans.ptx")

      // Read input file on master
      val timestamp0: Long = System.currentTimeMillis  
      val objects = read_file(input_file, numObjs, numCoords)
      //assert(objects.length != 0)
      val timestamp1: Long = System.currentTimeMillis 
      
      val objects_RDD = sc.parallelize(objects).coalesce(1) // run on 1 partition. RDD[(Float,String)]
      // Process it in parallel
      val model_RDD = objects_RDD.mapPartitions(x => mapPartitions_func(x, numCoords(0), numClusters, threshold, iterations)) //map()  // mapPartitions(Iterator[T]) 
      val model_arr = model_RDD.collect()  // Array of Models
      val model_any0 = model_arr(0) // Return from the 1 st element
      val model_any1 = model_arr(1) 
      val clusters= model_any0.asInstanceOf[Array[Float]]
      val membership = model_any1.asInstanceOf[Array[Int]]

      //val clusters = cuda_kmeans(objects, numCoords(0), numObjs(0), numClusters, threshold, membership, iterations)

      val timestamp2: Long = System.currentTimeMillis 

      write_file(output_file, numClusters, numObjs(0), numCoords(0), clusters, membership)
      val timestamp3: Long = System.currentTimeMillis 

      val read_file_time = (timestamp1 - timestamp0)
      val cuda_kmeans_time  = (timestamp2 - timestamp1)
      val write_file_time  = (timestamp3 - timestamp2)

      println("read_file: " + read_file_time + "ms")
      println("cuda_kmeans_time: " + cuda_kmeans_time + "ms")
      println("write_file_time: " + write_file_time + "ms")
      
    }

  }

  private def preparePtxFile(cuFileName: String): String = {
    var endIndex = cuFileName.lastIndexOf('.')
    if (endIndex == -1) {
      endIndex = cuFileName.length - 1
    }
    val ptxFileName = cuFileName.substring(0, endIndex + 1) + "ptx"
    val ptxFile = new File(ptxFileName)
    if (ptxFile.exists()) {
      return ptxFileName
    }
    val cuFile = new File(cuFileName)
    if (!cuFile.exists()) {
      throw new IOException("Input file not found: " + cuFileName)
    }
    val modelString = "-m" + System.getProperty("sun.arch.data.model")
    val command = "nvcc " + modelString + " -ptx " + cuFile.getPath + " -o " + 
      ptxFileName
    println("Executing\n" + command)
    val process = Runtime.getRuntime.exec(command)
    val errorMessage = new String(toByteArray(process.getErrorStream))
    val outputMessage = new String(toByteArray(process.getInputStream))
    var exitValue = 0
    try {
      exitValue = process.waitFor()
    } catch {
      case e: InterruptedException => {
        Thread.currentThread().interrupt()
        throw new IOException("Interrupted while waiting for nvcc output", e)
      }
    }
    if (exitValue != 0) {
      println("nvcc process exitValue " + exitValue)
      println("errorMessage:\n" + errorMessage)
      println("outputMessage:\n" + outputMessage)
      throw new IOException("Could not create .ptx file: " + errorMessage)
    }
    println("Finished creating PTX file")
    ptxFileName
  }

  def mapPartitions_func_test(t: Iterator[Float]): Iterator[Any]  = {
    var numObjs = 0
     while(t.hasNext) {
      numObjs += 1
      t.next()
    }
    return Iterator(numObjs)
  }

  def mapPartitions_func(t: Iterator[Float], numCoords: Int,  numClusters: Int, threshold: Float, iterations: Int): Iterator[Any] = {
    val objects = t.toArray
    val numObjs = objects.length / numCoords
    println(numObjs)
    println(numCoords)

    val membership = Array.ofDim[Int](numObjs)
    val clusters = cuda_kmeans(objects, numCoords, numObjs, numClusters, threshold, membership, iterations)
    return (clusters, membership).productIterator 
  }

  def mapped_func(t: Float): Float = {
   return t  
  }

  def get_index(y: Int, x: Int, width: Int): Int = {
    return x + y * width
  }

  def nextPowerOfTwo(n: Int): Int = {
      var i = n - 1
      i |= (i >>  1)
      i |= (i >>  2)
      i |= (i >>  4)
      i |= (i >>  8)
      i |= (i >> 16)
      //i |= (i >> 32)
      //i - (i >>> 1)
      return i + 1;
  }

  // out: [numClusters][numCoords]
  // objects: [numObjs][numCoords]
  def cuda_kmeans(objects: Array[Float], numCoords: Int, numObjs: Int, numClusters: Int, threshold: Float, membership: Array[Int], loop_iterations: Int): Array[Float] = {
    JCudaDriver.setExceptionsEnabled(true)

    val ptxFileName = SparkFiles.get("cuda_kmeans.ptx")
    //val ptxFileName = preparePtxFile("cuda_kmeans.cu")
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

    var i, j, index, loop = 0 
    val newClusterSize = Array.fill(numClusters){0} // number of objs assigned in each cluster
    var delta = 0.0f
    // [numCoords][numObjs] 
    val dimObjects = Array.ofDim[Float](numCoords * numObjs)
    // [numClusters][numCoords] 
    val clusters= Array.ofDim[Float](numClusters * numCoords)
    // [numCoords][numClusters]
    val dimClusters = Array.ofDim[Float](numCoords * numClusters)
    val newClusters = Array.fill(numCoords * numClusters){0.0f}

    val deviceObjects = new CUdeviceptr()
    val deviceClusters = new CUdeviceptr()
    val deviceMembership = new CUdeviceptr()
    val deviceIntermediates = new CUdeviceptr()

    /* initialize */
    for (i <- 0 until numCoords) {
      for (j <- 0 until numObjs) {
        dimObjects(get_index(i, j, numObjs)) = objects(get_index(j, i, numCoords)) 
      }
    }
    for (i <- 0 until numCoords) {
      for (j <- 0 until numClusters) {
        dimClusters(get_index(i, j, numClusters)) = dimObjects(get_index(i, j, numObjs)) 
      }
    }
    for (i <- 0 until numObjs) {
      membership(i) = -1
    }
    //  To support reduction, numThreadsPerClusterBlock *must* be a power of
    //  two, and it *must* be no larger than the number of bits that will
    //  fit into an unsigned char, the type used to keep track of membership
    //  changes in the kernel.
    val numThreadsPerClusterBlock = 128
    val numClusterBlocks = (numObjs + numThreadsPerClusterBlock - 1) / numThreadsPerClusterBlock;

    //#if BLOCK_SHARED_MEM_OPTIMIZATION
    val clusterBlockSharedDataSize = numThreadsPerClusterBlock * Sizeof.CHAR + numClusters * numCoords * Sizeof.FLOAT
    
    val deviceNum = Array(-1)
    JCuda.cudaGetDevice(deviceNum)
    val deviceProp = new cudaDeviceProp()
    JCuda.cudaGetDeviceProperties(deviceProp, deviceNum(0))
    if(clusterBlockSharedDataSize > deviceProp.sharedMemPerBlock) {
      println("WARNING: Your CUDA hardware has insufficient block shared memory.")
      println("You need to recompile with BLOCK_SHARED_MEM_OPTIMIZATION=0. ")
      println("See the README for details.")
    }

    //#else
    //const unsigned int clusterBlockSharedDataSize =numThreadsPerClusterBlock * sizeof(unsigned char);
    //#endif 

    val numReductionThreads = nextPowerOfTwo(numClusterBlocks)
    val reductionBlockSharedDataSize = numReductionThreads * Sizeof.INT

    cuMemAlloc(deviceObjects, numCoords * numObjs * Sizeof.FLOAT)
    cuMemAlloc(deviceClusters, numCoords * numClusters * Sizeof.FLOAT)
    cuMemAlloc(deviceMembership, numObjs * Sizeof.INT)
    cuMemAlloc(deviceIntermediates, numReductionThreads * Sizeof.INT) // unsigned data type

    cuMemcpyHtoD(deviceObjects, Pointer.to(dimObjects), numObjs * numCoords * Sizeof.FLOAT)
    cuMemcpyHtoD(deviceMembership, Pointer.to(membership), numObjs * Sizeof.INT)

    val timestamp1: Long = System.currentTimeMillis 
    do {
        cuMemcpyHtoD(deviceClusters, Pointer.to(dimClusters), numClusters * numCoords * Sizeof.FLOAT)

        val kernelParameters1 = Pointer.to(Pointer.to(Array(numCoords)), Pointer.to(Array(numObjs)), Pointer.to(Array(numClusters)),  Pointer.to(deviceObjects), Pointer.to(deviceClusters), Pointer.to(deviceMembership), Pointer.to(deviceIntermediates))

        // <<< numClusterBlocks, numThreadsPerClusterBlock, clusterBlockSharedDataSize >>>
        cuLaunchKernel(function1, numClusterBlocks, 1, 1, numThreadsPerClusterBlock, 1, 1, clusterBlockSharedDataSize, null, kernelParameters1, null)

        JCuda.cudaDeviceSynchronize()
        var e = JCuda.cudaGetLastError()
        if(e !=  cudaError.cudaSuccess) {
          printf("CUDA Error %d: %s\n", e, JCuda.cudaGetErrorString(e))
        }
        //val t = Array(0)
        //cuMemcpyDtoH(Pointer.to(t), deviceIntermediates, Sizeof.INT )
        //cuMemcpyDtoH(Pointer.to(membership), deviceMembership, numObjs * Sizeof.INT)
        //printf("numReductionThreads %d", numReductionThreads)
        //printf("imm %d\n", t(0))
        //for (i <- 0 until numObjs) {
        //  println(membership(i))
        //}

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

        for (i <- 0 until numObjs) {
          var index = membership(i)
          newClusterSize(index) += 1;
          for (j <- 0 until numCoords){
            //printf("i %d j %d index %d", i, j, index)
            //printf("newCluster %d idx %d", newClusters.length, get_index(j, index, numClusters))
            //printf("objects %d idx %d\n", objects.length, get_index(j, index, numObjs))
            newClusters(get_index(j, index, numClusters)) += objects(get_index(i, j, numCoords))
          }
        }
        
        //  TODO: Change layout of newClusters to [numClusters][numCoords]
        /* average the sum and replace old cluster centers with newClusters */
        for (i <- 0 until numClusters) {
          for (j <- 0 until numCoords) {
            if (newClusterSize(i) > 0) {
              dimClusters(get_index(j, i, numClusters)) = newClusters(get_index(j, i, numClusters)) / newClusterSize(i)
            }
            newClusters(get_index(j, i, numClusters)) = 0.0f 
          }
          newClusterSize(i) = 0
        }

        delta = delta / numObjs
        loop += 1
      } while (delta > threshold && loop < loop_iterations)

    val timestamp2: Long = System.currentTimeMillis 
    val train_time  = (timestamp2 - timestamp1)
    println("Within cuda_kmeans")

    println("\tloop_iterations: " + loop )
    println("\tdelta: " + delta )
    println("\ttrain: " + train_time + "ms" )

    for (i <- 0 until numClusters) {
      for (j <- 0 until numCoords) {
              clusters(get_index(i, j, numCoords)) = dimClusters(get_index(j, i, numClusters))
      }
    }

    cuMemFree(deviceObjects)
    cuMemFree(deviceClusters)
    cuMemFree(deviceMembership)
    cuMemFree(deviceIntermediates)
    
    return clusters
  }

  def write_file_hdfs (filename: String, numClusters: Int, numObjs: Int, numCoords: Int, clusters: Array[Float], membership: Array[Int]) {
    var buffer = new StringBuilder
    for(i <- 0 until numClusters){
      buffer ++= i + " "
      for(j <- 0 until numCoords){
        buffer ++= clusters(get_index(i, j, numCoords)) + " "
      }
      buffer ++= "\n"
    }
    sc.parallelize(buffer.toString.split("\n")).saveAsTextFile(filename + ".model")

    sc.parallelize(membership).zipWithIndex.map(case (c, i) => i + " " + c + "\n").saveAsTextFile(filename + ".membership")
  }

  def write_file (filename: String, numClusters: Int, numObjs: Int, numCoords: Int, clusters: Array[Float], membership: Array[Int]): Int = {

    var outFileName = filename + ".cluster_centres"
    printf("Writing coordinates of K=%d cluster centers to file \"%s\"\n", numClusters, outFileName)
    var writer = new PrintWriter(new File(outFileName))
 
    for(i <- 0 until numClusters){
      //printf("%d ", i)
      writer.write(i + " ")
      for(j <- 0 until numCoords){
        //printf("%f ", clusters(get_index(i, j, numCoords)))
        writer.write(clusters(get_index(i, j, numCoords)) + " ")
      }
      //printf("\n")
      writer.write("\n")
    }
    writer.close()

    outFileName = filename + ".membership"
    writer = new PrintWriter(new File(outFileName))
    printf("Writing membership of N=%d data objects to file \"%s\"\n", numObjs, outFileName)
    for(i <- 0 until numObjs){
      //printf("%d %d\n", i, membership(i))
      writer.write(i + " " + membership(i) + "\n" )
    }
    writer.close()
    return 0 
  }

  def read_file (filename: String, numObjs: Array[Int], numCoords: Array[Int]): Array[Float] = {

    // [numClusters][numCoords] 
    val source = scala.io.Source.fromFile(filename)
    //val source = scala.io.Source.fromFile("/Users/qijing.huang/Documents/CS267Project/kmeans/Image_data/color100.txt")
    val lines = (try source.mkString finally source.close()).split('\n').map(x => x.trim)
    numObjs(0) = lines.length
    val cols = lines(0).split(' ')

    numCoords(0) = cols.length - 1

    val objects = lines.flatMap(x => x.split(' ').slice(1, x.length).map(x => x.trim.toFloat))
    return objects
  }

  def read_file_2D (filename: String, numObjs: Array[Int], numCoords: Array[Int]): Array[Array[Float]] = {

    // [numClusters][numCoords] 
    val source = scala.io.Source.fromFile(filename)
    //val source = scala.io.Source.fromFile("/Users/qijing.huang/Documents/CS267Project/kmeans/Image_data/color100.txt")
    val lines = (try source.mkString finally source.close()).split('\n').map(x => x.trim)
    numObjs(0) = lines.length
    val cols = lines(0).split(' ')

    numCoords(0) = cols.length - 1

    val objects = lines.map(x => x.split(' ').slice(1, x.length).map(x => x.trim.toFloat))
    return objects
  }


  def cuda_predict (objects: Array[Float], clusters: Array[Float], numCoords: Int, numObjs: Int, numClusters: Int, membership: Array[Int]) {
    JCudaDriver.setExceptionsEnabled(true)

    //val ptxFileName = preparePtxFile("cuda_kmeans.cu")
   val ptxFileName = SparkFiles.get("cuda_kmeans.ptx") 
    println(ptxFileName) 
   cuInit(0)

    val device = new CUdevice()
    cuDeviceGet(device, 0)

    val context = new CUcontext()
    cuCtxCreate(context, 0, device)

    val module = new CUmodule()
    cuModuleLoad(module, ptxFileName)

    val function1 = new CUfunction()
    cuModuleGetFunction(function1, module, "_Z7predictiiiPfS_Pi")

    var i, j, index, loop = 0 
    val newClusterSize = Array.fill(numClusters){0} // number of objs assigned in each cluster
    var delta = 0.0f
    // [numCoords][numObjs] 
    val dimObjects = Array.ofDim[Float](numCoords * numObjs)
    // [numClusters][numCoords] 

    // [numCoords][numClusters]
    val dimClusters = Array.ofDim[Float](numCoords * numClusters)
    val newClusters = Array.fill(numCoords * numClusters){0.0f}

    val deviceObjects = new CUdeviceptr()
    val deviceClusters = new CUdeviceptr()
    val deviceMembership = new CUdeviceptr()

    /* initialize */
    for (i <- 0 until numCoords) {
      for (j <- 0 until numObjs) {
        dimObjects(get_index(i, j, numObjs)) = objects(get_index(j, i, numCoords)) 
      }
    }

    for (i <- 0 until numClusters) {
      for (j <- 0 until numCoords) {
        dimClusters(get_index(j, i, numClusters)) = clusters(get_index(i, j, numCoords))
      }
    }

    for (i <- 0 until numObjs) {
      membership(i) = -1
    }
    //  To support reduction, numThreadsPerClusterBlock *must* be a power of
    //  two, and it *must* be no larger than the number of bits that will
    //  fit into an unsigned char, the type used to keep track of membership
    //  changes in the kernel.
    val numThreadsPerClusterBlock = 128
    val numClusterBlocks = (numObjs + numThreadsPerClusterBlock - 1) / numThreadsPerClusterBlock;

    //#if BLOCK_SHARED_MEM_OPTIMIZATION
    val clusterBlockSharedDataSize = numThreadsPerClusterBlock * Sizeof.CHAR + numClusters * numCoords * Sizeof.FLOAT
    
    val deviceNum = Array(-1)
    JCuda.cudaGetDevice(deviceNum)
    val deviceProp = new cudaDeviceProp()
    JCuda.cudaGetDeviceProperties(deviceProp, deviceNum(0))
    if(clusterBlockSharedDataSize > deviceProp.sharedMemPerBlock) {
      println("WARNING: Your CUDA hardware has insufficient block shared memory.")
      println("You need to recompile with BLOCK_SHARED_MEM_OPTIMIZATION=0. ")
      println("See the README for details.")
    }

    //#else
    //const unsigned int clusterBlockSharedDataSize =numThreadsPerClusterBlock * sizeof(unsigned char);
    //#endif 

    val numReductionThreads = nextPowerOfTwo(numClusterBlocks)
    val reductionBlockSharedDataSize = numReductionThreads * Sizeof.INT

    cuMemAlloc(deviceObjects, numCoords * numObjs * Sizeof.FLOAT)
    cuMemAlloc(deviceClusters, numCoords * numClusters * Sizeof.FLOAT)
    cuMemAlloc(deviceMembership, numObjs * Sizeof.INT)

    cuMemcpyHtoD(deviceObjects, Pointer.to(dimObjects), numObjs * numCoords * Sizeof.FLOAT)
    cuMemcpyHtoD(deviceMembership, Pointer.to(membership), numObjs * Sizeof.INT)

    cuMemcpyHtoD(deviceClusters, Pointer.to(dimClusters), numClusters * numCoords * Sizeof.FLOAT)

    val kernelParameters1 = Pointer.to(Pointer.to(Array(numCoords)), Pointer.to(Array(numObjs)), Pointer.to(Array(numClusters)),  Pointer.to(deviceObjects), Pointer.to(deviceClusters), Pointer.to(deviceMembership))

    // <<< numClusterBlocks, numThreadsPerClusterBlock, clusterBlockSharedDataSize >>>
    cuLaunchKernel(function1, numClusterBlocks, 1, 1, numThreadsPerClusterBlock, 1, 1, clusterBlockSharedDataSize, null, kernelParameters1, null)

    JCuda.cudaDeviceSynchronize()
    var e = JCuda.cudaGetLastError()
    if(e !=  cudaError.cudaSuccess) {
      printf("CUDA Error %d: %s\n", e, JCuda.cudaGetErrorString(e))
    }

    cuMemcpyDtoH(Pointer.to(membership), deviceMembership, numObjs * Sizeof.INT)
    cuMemFree(deviceObjects)
    cuMemFree(deviceClusters)
    cuMemFree(deviceMembership)

    cuCtxDestroy(context)
  }

  def predict(inputFile: String, clusterFile: String, outFile: String) {
    val numObjs = Array(0) 
    val numCoords = Array(0) 
    val numClusters = Array(0)

    val clusterNumCoords = Array(0)
    
    val objects = read_file(inputFile, numObjs, numCoords)
    val centers = read_file(clusterFile, numClusters, clusterNumCoords)
    println("Num clusters: " + numClusters(0))
    println("Num coords: " + numCoords(0))
    assert (clusterNumCoords(0) == numCoords(0))

    val membership = predict(objects, centers, numObjs(0), numClusters(0), numCoords(0))

    val outFileName = outFile + ".membership"
    val writer = new PrintWriter(new File(outFileName))
    printf("Writing membership of N=%d data objects to file \"%s\"\n", numObjs(0), outFileName)
    for(i <- 0 until numObjs(0)) {
      //printf("%d %d\n", i, membership(i))
      writer.write(i + " " + membership(i) + "\n" )
    }
    writer.close()
  }

  // objects and center arrays do not contain the indices.
  // Returns the membership array
  def predict(objects:Array[Float], centers:Array[Float], numObjs: Int, numClusters: Int, numFeatures:Int): Array[Int] = {
    assert(objects.length != 0)
    val membership = Array.ofDim[Int](numObjs)
    val timestamp0: Long = System.currentTimeMillis
    cuda_predict(objects, centers, numFeatures, numObjs, numClusters, membership)
    val timestamp1: Long = System.currentTimeMillis
    val predict_time = (timestamp1 - timestamp0)
    println("predict: " + predict_time + "ms")
    membership
  }

  private def toByteArray(inputStream: InputStream): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val buffer = Array.ofDim[Byte](8192)
    while (true) {
      val read = inputStream.read(buffer)
      if (read == -1) {
        //break
      }
      baos.write(buffer, 0, read)
    }
    baos.toByteArray()
  }
}



















package project_2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.rdd._
import org.apache.spark.rdd.RDD

object main {

  val seed = new java.util.Date().hashCode
  val rand = new scala.util.Random(seed)

  class hash_function(numBuckets_in: Long) extends Serializable {
    val p: Long = 2147483587
    val a: Long = (rand.nextLong % (p - 1)) + 1
    val b: Long = rand.nextLong % p
    val numBuckets: Long = numBuckets_in

    def convert(s: String, ind: Int): Long = {
      if (ind == 0) return 0
      (s(ind - 1).toLong + 256 * convert(s, ind - 1)) % p
    }

    def hash(s: String): Long = ((a * convert(s, s.length) + b) % p) % numBuckets

    def hash(t: Long): Long = ((a * t + b) % p) % numBuckets

    def zeroes(num: Long, remain: Long): Int = {
      if ((num & 1) == 1 || remain == 1) return 0
      1 + zeroes(num >> 1, remain >> 1)
    }

    def zeroes(num: Long): Int = zeroes(num, numBuckets)
  }

  class four_universal_Radamacher_hash_function extends hash_function(2) {
    override val a: Long = rand.nextLong % p
    override val b: Long = rand.nextLong % p
    val c: Long = rand.nextLong % p
    val d: Long = rand.nextLong % p

    override def hash(s: String): Long = {
      val t = convert(s, s.length)
      val t2 = t * t % p
      val t3 = t2 * t % p
      if (((a * t3 + b * t2 + c * t + d) % p & 1) == 0) 1 else -1
    }

    override def hash(t: Long): Long = {
      val t2 = t * t % p
      val t3 = t2 * t % p
      if (((a * t3 + b * t2 + c * t + d) % p & 1) == 0) 1 else -1
    }
  }

  class BJKSTSketch(bucket_in: Set[(String, Int)], z_in: Int, bucket_size_in: Int) extends Serializable {

    var bucket: Set[(String, Int)] = bucket_in

    var z: Int = z_in

    val BJKST_bucket_size = bucket_size_in



    // Constructor overload to initialize the sketch with a single string and its zero count.

    def this(s: String, z_of_s: Int, bucket_size_in: Int) {

      this(Set((s, z_of_s)), z_of_s, bucket_size_in)
    }


    // Adds a string and its zero count to the sketch, applying the necessary logic for bucket management.

    def add_string(s: String, z_of_s: Int): BJKSTSketch = {

      if (z_of_s >= z) {

        bucket += ((s, z_of_s))

        shrinkBucketIfNeeded()

        }

      this

      }



    // Combines two sketches, updating the bucket and z value as needed.

    def +(that: BJKSTSketch): BJKSTSketch = {

      var combinedBucket = this.bucket ++ that.bucket

      this.bucket = combinedBucket

      this.z = combinedBucket.foldLeft(this.z)((acc, pair) => Math.max(acc, pair._2))

      shrinkBucketIfNeeded()

      this

      }



    // Private method to shrink the bucket when its size exceeds the specified threshold.

    private def shrinkBucketIfNeeded(): Unit = {

      while (bucket.size >= BJKST_bucket_size) {

        z += 1

        bucket = bucket.filter(_._2 >= z)

        }

      }



    // Calculates the estimate based on the current state of the bucket and z value.

    def estimate: Double = {

      bucket.size * Math.pow(2, z)

      }

  }



  def tidemark(x: RDD[String], trials: Int): Double = {

    val h = Seq.fill(trials)(new hash_function(2000000000))



    def param0 = (accu1: Seq[Int], accu2: Seq[Int]) => Seq.range(0, trials).map(i => scala.math.max(accu1(i), accu2(i)))

    def param1 = (accu1: Seq[Int], s: String) => Seq.range(0, trials).map(i => scala.math.max(accu1(i), h(i).zeroes(h(i).hash(s))))



    val x3 = x.aggregate(Seq.fill(trials)(0))(param1, param0)

    val ans = x3.map(z => scala.math.pow(2, 0.5 + z)).sortWith(_ < _)(trials / 2) // Take the median of the trials



    ans

  }



  def BJKSTSketch(x: RDD[String], width: Int, trials: Int): Double = {
    // Initialize an array to hold the sketches
    val sketches = Array.fill(trials)(new BJKSTSketch(Set.empty[(String, Int)], 0, width))


    // Broadcast the sketches array to be accessible across the cluster
    val sparkContext = x.sparkContext
    val sketchesBroadcast = sparkContext.broadcast(sketches)

    // Map each string in the RDD to a tuple of (sketchIndex, (string, zeroCount))
    val hashedRDD = x.flatMap { s =>
      (0 until trials).map { i =>
        val hashFunc = new hash_function(2000000000L) // Assuming a large number of buckets for the hashing
        val zeroCount = hashFunc.zeroes(hashFunc.hash(s))
        (i, (s, zeroCount))
      }
    }

    // Group by sketchIndex to aggregate strings and their zero counts per sketch
    val groupedBySketchIndex = hashedRDD.groupByKey()

    // Update each sketch with its corresponding strings and zero counts
    groupedBySketchIndex.foreach { case (sketchIndex, stringsAndZeroCounts) =>
      val sketch = sketchesBroadcast.value(sketchIndex)
      stringsAndZeroCounts.foreach { case (s, zeroCount) =>
        sketch.add_string(s, zeroCount)
      }
    }

    // Collect estimates from each sketch and compute the median
    val estimates = sketchesBroadcast.value.map(_.estimate).sorted
    val medianEstimate = if (estimates.length % 2 == 0) {
      val midIndex = estimates.length / 2
      (estimates(midIndex - 1) + estimates(midIndex)) / 2.0
    } else {
      estimates(estimates.length / 2)
    }

    medianEstimate
  }





  def Tug_of_War(x: RDD[String], width: Int, depth: Int): Long = {
    val rand = new scala.util.Random(seed)

    // Define a hash function that maps to +1 or -1
    class ToWHashFunction extends Serializable {
      val p: Long = 2147483587 // A large prime number
      val a: Long = rand.nextLong() % p
      val b: Long = rand.nextLong() % p

      def hash(s: String): Int = {
        val hashValue = (a * s.hashCode + b) % p
        if (hashValue % 2 == 0) 1 else -1
      }
    }

    // Generate 'depth' hash functions
    val hashFunctions = Array.fill(depth)(new ToWHashFunction)

    // Map phase: Compute the sketch for each element
    val sketches = x.map { s =>
      hashFunctions.map(_.hash(s))
    }

    // Reduce phase: Sum the sketches element-wise
    val sketchSum = sketches.reduce { (a, b) =>
      (a zip b).map { case (x, y) => x + y }
    }

    // Compute the average of squares of the sketch sums
    val f2Estimate = sketchSum.map(x => x * x.toLong).sum / depth

    f2Estimate
  }





  def exact_F0(x: RDD[String]) : Long = {
    val ans = x.distinct.count
    return ans
  }


  def exact_F2(x: RDD[String]): Long = {
    val countMap = x.map(s => (s, 1L)).reduceByKey(_ + _)
    countMap.map { case (_, count) => count * count }.sum().toLong
  }



  def main(args: Array[String]) {

    // Initialize SparkSession with master URL
    val spark = SparkSession.builder()
      .appName("Project_2")
      .master("local[*]")
      .getOrCreate()


    if(args.length < 2) {
      println("Usage: project_2 input_path option = {BJKST, tidemark, ToW, exactF2, exactF0} ")
      sys.exit(1)
    }
    val input_path = args(0)

  //    val df = spark.read.format("csv").load("data/2014to2017.csv")
    val df = spark.read.format("csv").load(input_path)
    val dfrdd = df.rdd.map(row => row.getString(0))

    val startTimeMillis = System.currentTimeMillis()

    if(args(1)=="BJKST") {
      if (args.length != 4) {
        println("Usage: project_2 input_path BJKST #buckets trials")
        sys.exit(1)
      }
      val ans = BJKSTSketch(dfrdd, args(2).toInt, args(3).toInt)

      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000

      println("==================================")
      println("BJKST Algorithm. Bucket Size:"+ args(2) + ". Trials:" + args(3) +". Time elapsed:" + durationSeconds + "s. Estimate: "+ans)
      println("==================================")
    }
    else if(args(1)=="tidemark") {
      if(args.length != 3) {
        println("Usage: project_2 input_path tidemark trials")
        sys.exit(1)
      }
      val ans = tidemark(dfrdd, args(2).toInt)
      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000

      println("==================================")
      println("Tidemark Algorithm. Trials:" + args(2) + ". Time elapsed:" + durationSeconds + "s. Estimate: "+ans)
      println("==================================")

    }
    else if(args(1)=="ToW") {
       if(args.length != 4) {
         println("Usage: project_2 input_path ToW width depth")
         sys.exit(1)
      }
      val ans = Tug_of_War(dfrdd, args(2).toInt, args(3).toInt)
      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
      println("==================================")
      println("Tug-of-War F2 Approximation. Width :" +  args(2) + ". Depth: "+ args(3) + ". Time elapsed:" + durationSeconds + "s. Estimate: "+ans)
      println("==================================")
    }
    else if(args(1)=="exactF2") {
      if(args.length != 2) {
        println("Usage: project_2 input_path exactF2")
        sys.exit(1)
      }
      val ans = exact_F2(dfrdd)
      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000

      println("==================================")
      println("Exact F2. Time elapsed:" + durationSeconds + "s. Estimate: "+ans)
      println("==================================")
    }
    else if(args(1)=="exactF0") {
      if(args.length != 2) {
        println("Usage: project_2 input_path exactF0")
        sys.exit(1)
      }
      val ans = exact_F0(dfrdd)
      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000

      println("==================================")
      println("Exact F0. Time elapsed:" + durationSeconds + "s. Estimate: "+ans)
      println("==================================")
    }
    else {
      println("Usage: project_2 input_path option = {BJKST, tidemark, ToW, exactF2, exactF0} ")
      sys.exit(1)
    }

  }
}


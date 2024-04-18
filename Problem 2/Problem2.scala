import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import java.io._ // needed to store the list of run times as a text file

object SparkWordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Correct arguments: <input-directory> <output-directory>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("SparkWordCount")
    val ctx = new SparkContext(sparkConf) 
    val textFile = ctx.textFile(args.slice(0, args.length-1).mkString(","))
    var startRunTime = System.currentTimeMillis
    var endRunTime: Long = -1 // simply to define it
    var RunTimeList: List[String] = List()  // we initiliaze a list to keep track of all the run times

    // Question a1-2: Tokenize and count individual words
    val Qa = textFile
      .flatMap(line => line.toLowerCase.split("\\s+")) // Split all the words separated by one or more white space into distinct strings
      .filter(word => word.matches("[a-z]{5,25}") || word.matches("[0-9]{2,12}")) // Filter words with length 5-25 and number with 2 to 12 digits
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    endRunTime = System.currentTimeMillis - startRunTime // we compute the run time it took to run this portion of the code
    startRunTime = System.currentTimeMillis // we reinitialize the start of the run time
    RunTimeList = RunTimeList :+ (endRunTime.toString + "\n") // we add the run time of this job to the list

    // Question b1: Find all individual words (not numbers) with a count of exactly 1,000.
    val Qb1 = textFile
      .flatMap(line => line.toLowerCase.split("\\s+")) // Split all the words into distinct strings
      .filter(word => word.matches("[a-z]{5,25}")) // Filter words with length 5-25
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .filter(x => x._2 == 1000)

    endRunTime = System.currentTimeMillis - startRunTime // we compute the run time it took to run this portion of the code
    startRunTime = System.currentTimeMillis // we reinitialize the start of the run time
    RunTimeList = RunTimeList :+ (endRunTime.toString + "\n") // we add the run time of this job to the list


    // Question b2: Find all word pairs (not numbers) with a count of exactly 1,000.
    val Qb2 = textFile
      .flatMap(line => line.toLowerCase.split("\\s+").toList.sliding(2)) // Split all the words into distinct strings, and then go through them as a list and create pairs
      .filter(pair => pair.head.matches("[a-z]+") && pair.last.matches("[a-z]+")) // Filter pairs of words (regardless of length)
      .map(pair => (pair.mkString(" "), 1)) // turn pairs into a single string
      .reduceByKey(_ + _)
      .filter(x => x._2 == 1000)

    endRunTime = System.currentTimeMillis - startRunTime // we compute the run time it took to run this portion of the code
    startRunTime = System.currentTimeMillis // we reinitialize the start of the run time
    RunTimeList = RunTimeList :+ (endRunTime.toString + "\n") // we add the run time of this job to the list

    // Question b3: Find the top-100 most frequent individual words (not numbers).
    val Qb3 = textFile
      .flatMap(line => line.toLowerCase.split("\\s+")) // Split all the words into distinct strings
      .filter(word => word.matches("[a-z]+")) // Filter words
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .sortBy(x => x._2, ascending = false) // we sort in descending order, by count, all the words
      .take(100) // we take the top 100 most frequent words

    endRunTime = System.currentTimeMillis - startRunTime // we compute the run time it took to run this portion of the code
    startRunTime = System.currentTimeMillis // we reinitialize the start of the run time
    RunTimeList = RunTimeList :+ (endRunTime.toString + "\n") // we add the run time of this job to the list

    // Question b4: Find the top-100 most frequent pairs of tokens, where the first token is a number and the second token is a word.
    val Qb4 = textFile
      .flatMap(line => line.toLowerCase.split("\\s+").toList.sliding(2)) // Split all the words into distinct strings, and then go through them as a list and create pairs
      .filter(pair => pair.head.matches("[0-9]+") && pair.last.matches("[a-z]+")) // Filter pairs of numbers + words (again regardless of their length)
      .map(pair => (pair.mkString(" "), 1)) // turn pairs into a single string
      .reduceByKey(_ + _)
      .sortBy(x => x._2, ascending = false) // we sort in descending order, by count, all the pairs
      .take(100) // we take the top 100 most frequent pairs

    endRunTime = System.currentTimeMillis - startRunTime // we compute the run time it took to run this portion of the code
    RunTimeList = RunTimeList :+ endRunTime.toString // we add the run time of this job to the list


    // Save results to output directories
    Qb1.saveAsTextFile(args.last + "/QuestionB1")
    Qb2.saveAsTextFile(args.last + "/QuestionB2")
    ctx.parallelize(Qb3).saveAsTextFile(args.last + "/QuestionB3")
    ctx.parallelize(Qb4).saveAsTextFile(args.last + "/QuestionB4")

    val file = "RunTimeList.txt"
    val writer = new BufferedWriter(new FileWriter(file))
    RunTimeList.foreach(writer.write(_) + "\n") // writing elements with newline separator for readability
    writer.close()

    ctx.stop()
    println(RunTimeList)
  }
}

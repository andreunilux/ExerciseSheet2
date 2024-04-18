import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions
class Main {
  val dataframe = spark.read.option("inferSchema", true).option("header", true).csv("../Data/titanic.csv")
  dataframe.show
  dataframe.createOrReplaceTempView("titanic")

  //a)

  val avg_age = dataframe.select(mean(dataframe("Age"))).collect()(0)(0)
  val filtered_names = dataframe.filter($"Age" > avg_age).select("Name").distinct.sort("Name")
  filtered_names.show()

  //b)

  val names = spark.sql("SELECT name FROM (SELECT DISTINCT name, age, avg(age) OVER(PARTITION BY 1) AS avg_age FROM titanic) WHERE age > avg_age").sort("Name").show()

  //c)

  case class Average(var sum: Long, var count: Long)

  object AgeAverage extends Aggregator[Long, Average, Double] {
    // A zero value for this aggregation. Should satisfy the property that any b + zero = b
    def zero: Average = Average(0L, 0L)
    // Combine two values to produce a new value. For performance, the function may modify `buffer`
    // and return it instead of constructing a new object
    def reduce(buffer: Average, data: Long): Average = {
      if (data == null) {
        val data = 23L
      }
      buffer.sum += data
      buffer.count += 1
      buffer
    }
    // Merge two intermediate values
    def merge(b1: Average, b2: Average): Average = {
      b1.sum += b2.sum
      b1.count += b2.count
      b1
    }
    // Transform the output of the reduction
    def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count
    // Specifies the Encoder for the intermediate value type
    def bufferEncoder: Encoder[Average] = Encoders.product
    // Specifies the Encoder for the final output value type
    def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }

  //d)
  def toDouble: (Any) => Double = { case i: Int => i case f: Float => f case d: Double => d }

  val avg_age_alive = toDouble(dataframe.filter($"Survived" > 0).select(mean(dataframe("Age"))).collect()(0)(0))
  val avg_age_dead = toDouble(dataframe.filter($"Survived" < 1).select(mean(dataframe("Age"))).collect()(0)(0))
  println("Testing the hypothesis:")
  println("\"The average age of the survivors of the Titanic is higher than that of the deceased.\"")
  if (avg_age_alive > avg_age_dead){
    println("The hypothesis is true")
  } else if(avg_age_alive < avg_age_dead){
    println("The hypothesis is false")
  } else{
    println("The result is unconclusive")
  }
}
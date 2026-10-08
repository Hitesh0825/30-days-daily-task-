import org.apache.spark.sql.SparkSession

case class Employee(id:Int,name:String,department:String,salary:Double)

object Main {
  def main(args:Array[String]):Unit = {
    val spark = SparkSession.builder.appName("Day14-DataFrame-Dataset").master("local[*]").getOrCreate()
    import spark.implicits._

    val ds = Seq(
      Employee(1,"A","IT",60000),
      Employee(2,"B","HR",50000),
      Employee(3,"C","IT",75000)
    ).toDS()

    println("Dataset:")
    ds.show()

    println("Dataset converted to DataFrame:")
    ds.toDF().show()

    println("RDD = low-level distributed data")
    println("DataFrame = untyped structured data")
    println("Dataset = typed structured data")
    println("Dataset provides compile-time type safety; DataFrame/Dataset use Catalyst optimization.")

    spark.stop()
  }
}

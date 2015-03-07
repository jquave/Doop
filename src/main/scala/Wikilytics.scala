
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object Wikilytics {

    def main(args: Array[String]) {

        val usage = """
            wikilytics [inputfile] [outputfile]
        """

        if(args.length<2) {
            println(usage)
        }
        else {

            val conf = new SparkConf().setAppName("Wikilytics Scala Spark")
            val sc = new SparkContext(conf)

            val wikidata = sc.textFile("/home/jquave/hadoop/dataSets/pc")

            //val enLines = data.filter(line => line.startsWith("en"))
            //println("English entries: %s", enLines.count())

            val wdata = wikidata.filter(f=>f.startsWith("en ")).map(f=>f.split(" ")).groupBy(g=>g(1)).map(f => (f._1,f._2.map(g=>g(2)) ) ).map(f=> (f._2.map(g=>g.toInt).sum, f._1) )
            val wdatasorted = wdata.sortByKey(false)
            wdatasorted.saveAsTextFile("wdatasorted")
        }
    }
}

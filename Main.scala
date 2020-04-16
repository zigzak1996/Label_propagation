import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    val topcats_path = args(0)
    val page_names_path = args(1)
    val categories_path = args(2)
    val spark = SparkSession
      .builder()
      .appName("Assignment 3")
      .config("spark.master", "local[6]")
      .getOrCreate()
    val sc = spark.sparkContext

    //    Data acquisition part:
    //      Predefine the number of categories to read and create the graph
    //      Fix the random seed to reproduce the results
    val categories_num = 1200
    val seed = 21
    val (graph, vert_cat) = read_graph(sc, categories_num, seed, topcats_path, page_names_path, categories_path)

    println("Built graph has: ")
    println("Basic categories: " + categories_num)
    println("Vertices: " + graph.vertices.count())
    println("Edges: " + graph.edges.count())
    println

    //    Perform LPA
    var t0 = System.nanoTime()
    val newGraph = CustomLPA.run(graph, EdgeDirection.Both, tresh = 0.0, maxIter = 10)
    var t1 = System.nanoTime()
    println("LPA execution time: " + (t1 - t0) / 1000000000.0 + " s\n")

    println("Forming clusters distribution sorted list..")
    t0 = System.nanoTime()
    //    Create Map of the form cluster_label:num_of_elements
    val label = newGraph.vertices.map(x => (x._2, 1.0)).reduceByKey(_ + _) //.collect().toMap

    //    Filter the marginal clusters of size one
    val filtered_labels = label.filter(_._2 > 1)
    val filtered_size = filtered_labels.count()

    //    Calculate the statistics of clusters
    val clusters_num: Long = filtered_size
    val unf_clusters_num: Long = label.count()
    val mean: Double = filtered_labels.map(_._2).reduce(_ + _) / filtered_size
    val std: Double = math.sqrt(filtered_labels.map(x => math.pow(x._2 - mean, 2.0)).reduce(_ + _) / filtered_size)
    val med: Double = median(filtered_labels.map(_._2).collect().toList)


    val print_top_n = 25
    //    Form the list of a form cluster:cluster_categories
    //      and take print_top_n big clusters to analyse
    val filtered_labels_bc = sc.broadcast(filtered_labels.collect().toMap)
    val clust_list = sc.parallelize(newGraph.vertices
      .filter(x => filtered_labels_bc.value.contains(x._2))
      .map(x => (x._2, vert_cat(x._1).toList))
      .reduceByKey(_ ++ _)
      .sortBy(x => -1 * filtered_labels_bc.value(x._1))
      .take(print_top_n)
      .drop(3))

    //    Calculate the probability distribution of clusters on categories
    println("Cluster + cluster size + categories distribution")
    val top_clusters = clust_list
      .map(x => {
        val cat_list = x._2.map(cat => (cat, 1.0 / x._2.size))
          .groupBy(_._1)
          .mapValues(seq => seq.reduce((x, y) => (x._1, x._2 + y._2)))
          .map(_._2)

        (x._1, filtered_labels_bc.value(x._1), cat_list.toList.filter(_._2 > 0.001).sortBy(-1 * _._2))
      })
      .sortBy(-1 * _._2)
    t1 = System.nanoTime()
    println("Done sorting in  " + (t1 - t0) / 1000000000.0 + " s\n")
    top_clusters.foreach(println)
    println


    println("Clusters' statistics are")
    println("Unfiltered clusters num: " + unf_clusters_num)
    println("Clusters num: " + clusters_num)
    println("Mean: " + mean)
    println("Median: " + med)
    println("STD: " + std)
    println("Max: " + filtered_labels.collect().maxBy(_._2)._2)
    println("Min: " + filtered_labels.collect().minBy(_._2)._2)

    println
    println("Top clusters analysis:")

    val collected_top_clusters = top_clusters.collect()
    // Perform the analytics of clusters on their correspondence to covered categories
    analyse_cluster_category(newGraph, collected_top_clusters(0), sc, categories_path)
    analyse_cluster_category(newGraph, collected_top_clusters(7), sc, categories_path)
    analyse_cluster_category(newGraph, collected_top_clusters(19), sc, categories_path)

  }

  def metrics_for_category(categories_file: RDD[String], categories_num: Int, seed: Long): Unit = {

    val dataset = categories_file
      .map(x => (x.split(" ")(0), x.split(" ").drop(1).map(_.toInt).size))
      .takeSample(withReplacement = false, num = categories_num, seed = seed)

    val mean: Double = dataset.map(_._2).reduce(_ + _).toDouble / dataset.size.toDouble
    val std: Double = math.sqrt(dataset.map(x => math.pow(x._2 - mean, 2.0)).reduce(_ + _) / dataset.size)
    val med: Double = median(dataset.map(_._2.toDouble).toSeq)
    println("Statistic of initial graph")
    println("Mean: " + mean)
    println("Median: " + med)
    println("STD: " + std)
    println("Max: " + dataset.maxBy(_._2)._2)
    println("Min: " + dataset.minBy(_._2)._2)

  }

  def read_graph(
          sc: SparkContext,
          categories_num: Int,
          seed: Long,
          topcats_path: String,
          page_names_path: String,
          categories_path: String
          ) = {

    val categories_file = sc.textFile(categories_path)
    //    Define all the nodes presented in the randomly chosen with seed categories

    metrics_for_category(categories_file, categories_num, seed)

    val categories: Set[Int] = categories_file
      .map(x => x.split(" ").drop(1).map(_.toInt).toSet)
      .takeSample(withReplacement = false, num = categories_num, seed = seed)
      .reduce(_.union(_))

    //    Create the map between Vertex and all its categories
    val vert_cat = read_categories(categories_file, sc)

    //    Read and filter vertices by categories
    val vertices: RDD[(VertexId, String)] = sc.textFile(page_names_path)
      .map(x => (x.split(" ")(0).toLong, x.split(" ").drop(1).mkString(" ")))
      .filter(x => categories.contains(x._1.toInt))

    //    Read and filter edges by categories
    val edges: RDD[Edge[Long]] = sc.makeRDD(
      sc
        .textFile(topcats_path)
        .map(x => (x.split(" ")(0).toLong, x.split(" ")(1).toLong))
        .filter(x => categories.contains(x._1.toInt))
        .filter(x => categories.contains(x._2.toInt))
        .map(x => Edge(x._1, x._2, 1L)).collect()
    )
    (Graph(vertices, edges), vert_cat)
  }

  def analyse_cluster_category(
          graph: Graph[VertexId, Long],
          cluster: (VertexId, Double, List[(String, Double)]),
          sc: SparkContext,
          categories_path: String
          ) = {
    println("Given cluster:")
    println("Label: " + cluster._1 + ", number of elements: " + cluster._2)
    println("Relevant categories:\n")
    println(cluster._3)
    println
    println("Comparison to the categories:")
    val categories = sc.textFile(categories_path)
      .map(line => {
        val splt_line = line.split(" ")
        (splt_line(0), splt_line.drop(1).map(_.toLong))
      }).collect().toMap
    for (cat <- cluster._3.take(5)) {
      val curr_category = categories(cat._1)
      val cluster_vertices = graph.vertices.filter(_._2 == cluster._1).map(_._1).collect().toSet
      println(cat._1 + ", number of elements: " + curr_category.size, " coverage by cluster: " + curr_category.toSet.intersect(cluster_vertices).size / curr_category.size.toFloat)

    }
    println
  }

  def median(s: Seq[Double]) = {
    val (lower, upper) = s.sortWith(_ < _).splitAt(s.size / 2)
    if (s.size % 2 == 0) (lower.last + upper.head) / 2.0 else upper.head
  }

  def read_categories(cat_file: RDD[String], sc: SparkContext) = {
    var vert_cats = Array[(VertexId, Set[String])]()
    cat_file.collect().foreach(line => {
      val splt = line.split(" ")
      vert_cats = vert_cats ++ splt.drop(1).map(vert => (vert.toLong, Set(splt(0))))
    })

    vert_cats.groupBy(_._1).mapValues(seq => seq.reduce { (x, y) => (x._1, x._2.union(y._2)) }).map(_._2)
  }
}

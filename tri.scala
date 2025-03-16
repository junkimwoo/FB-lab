
object Tri {
   def main(args: Array[String]) = {  // this is the entry point to our code
      val sc = getSC()  // one function to get the sc variable
      val myrdd = getFB(sc) // on function to get the rdd
      val counts = countTriangles(myrdd) // get the number of triangles
      //sadly we convert this single number into an rdd and save it to HDFS
      sc.parallelize(List(counts)).saveAsTextFile("NumberOfTriangles")  
  }

    def getSC() = {
      val conf = new SparkConf().setAppName("tc")
      val sc = new SparkContext(conf)
      sc// get the spark context variable
    }

    def getFB(sc: SparkContext): RDD[(String, String)] = {
      val lines = sc.textFile("/datasets/facebook")
      val sp = lines.map(_.split(" "))
      val part = sp.map(parts => (parts(0), parts(1)))
      part

        // read the /datasets/facebook data and convert each line into an
        // rdd. Each entry of the RDD is a pair of strings, representing the ids
        // of nodes that are neighbors
        //
        // Remember to split by a single space, not by a tab
    }

    def makeRedundant(edgeList: RDD[(String, String)]): RDD[(String, String)] = {
      val expand = edgeList.flatMap{ case (a,b) => Iterator((a,b), (b,a)) }
      val redundant = expand.distinct()
      redundant

        // An edge list is redundant if whenver an entry (a,b) appears, then so does (b,a)
        // If input is this:
        //    (1, 2)
        //    (3, 1) 
        //    (1, 2)
        //    (3, 2)
        // Then the output should be this (ordering of the rows does not matter)
        // (2, 1) 
        // (1, 2) 
        // (1, 3)
        // (3, 1)
        // (3, 2)
        // (2, 3)
        // there are no duplicates
        // This can be done using 2 total transformations
    }

   def noSelfEdges(edgeList: RDD[(String, String)]): RDD[(String, String)] = {
     val filterSelfEdges = edgeList.filter { case (a, b) => a != b}
     filterSelfEdges
        //Gets rid of self-edges
        // If the input rdd is this:
        // (1, 2)
        // (1, 1)
        // The output RDD would be
        // (1, 2)
        
        // this can be done using 1 transformation
   }


   def friendsOfFriends(edgeList: RDD[(String, String)]): RDD[(String, (String, String))] = {
     val commonFriends = edgeList.join(edgeList).filter { case (_, (x,y)) => x != y}
     commonFriends
       // From the edge list, we want to know which nodes have friends in common
       // If an input RDD looks like this
       // (1, 2)
       // (2, 3)
       // (2, 1)
       // (3, 2)
       // We want the output to look like this
       // (2, (1, 3)    <---  this means there is an edge from 1 to 2 and an edge from 2 to 3
       // (2, (3, 1)    <--- this means there is an edge from 3 to 2 and from 2 to 1

       // this is the same as finding all paths of length 2 in the graph specified by edgeList.


       // You can essume that the input edgeList is in redundant form
       // you only need 1 wide dependency operation. In fact, you only need 1 transformation total.
   }

   def journeyHome(edgeList: RDD[(String, String)],  twoPaths:  RDD[(String, (String, String))]): RDD[((String, String), (String, Null))] = {
     val edgeKeys = edgeList.map { case(a, b) => ((a, b), null)}
     val pathKeys = twoPaths.map { case(a, b) => (b, a)}
     val joined = pathKeys.join(edgeKeys)
     joined

       // There are two input RDDs. The first is an edgeList, like this
       // (1, 2)
       // (1, 3)
       // (3, 1)
       // (4, 2)
       // (4, 1)
       // and the second is a list of paths of length 2 like this
       // (2, (1, 3))    <--- means there is a path from 1 to 2 to 3
       // (2, (3, 1))    <--- means there is a path from 3 to 2 to 1
       // (5, (1, 4))    <--- means there is a path from 1 to 5 to 4
       // (6, (4, 2))
       //
       // We would like to join together all entries from the first RDD that match the
       // last tuple of the second RDD. For example, we would like to 
       // match the (1,3) to (2, (1, 3)) and
       // match the (3,1) to (2, (3, 1))
       //
       // You will use join to do the match, but you will need to create some intermediate
       // RDDs and think carefully about what their keys and values should be.
       //
       //The output should look like this:
       //
       // ((1, 3), (2, null))  <---- this is the result of matching (1,3) to (2, (1, 3))
       // ((3, 1), (2, null))  <---- this is the result of matching (3,1) to (2, (3, 1))
       // ((4, 2), (6, null))  <---- this is the result of matching (2,4) to (6, (4, 2))
   } 

   def toyGraph(sc: SparkContext): RDD[(String, String)] = {
       // creates a toy graph for triangle counting
       //
       // 1 ----- 2
       // | \     |
       // |   \   |
       // |     \ |
       // 4-------3 ------ 5
       //
       // There are only 2 triangles (a triangle is a group of 3 nodes that have edges between them)
       //
       val mylist = List[(String, String)](
                         ("1", "2"),
                         ("2", "1"),
                         ("2", "3"),
                         ("3", "2"),
                         ("1", "3"),
                         ("3", "1"),
                         ("1", "4"),
                         ("4", "1"),
                         ("4", "3"),
                         ("3", "4"),
                         ("3", "5"),
                         ("5", "3"),
                         // add some tricky things
                         ("1", "3"), // duplicate
                         ("3", "1"),
                         ("1", "1"),  //self edge
                         ("3", "5"),
                         ("5", "3"),
                         ("1", "3"),
                         ("3", "1"),
                         ("1", "4"),
                         ("4", "1"),
                         ("4", "3")
                        )
        sc.parallelize(mylist, 2)
    }

    def countTriangles(edgeList: RDD[(String, String)]) = {
      val noSelfEdgesRDD = noSelfEdges(edgeList)
      val redundantEdges = makeRedundant(noSelfEdgesRDD)
      val fofRDD = friendsOfFriends(redundantEdges)
      val almostThere = journeyHome(redundantEdges, fofRDD)
      almostThere.count() / 6
    }


  
    
    


}
                                                                                                                          



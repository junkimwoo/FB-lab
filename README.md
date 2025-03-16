# PySpark-Based Facebook Graph Analysis

## Project Overview  
This project analyzes Facebook Graph data using PySpark and MapReduce to count the number of triangles in a social network.
code is available at tri.scala
Used FaceBook data that was given by DS410 course at PSU.

## Key Features  
**Big Data Processing with PySpark:** Utilizes RDD transformations for efficient distributed computing  
**Graph Analysis with Triangle Counting:** Implements an algorithm to detect and count triangles in a social network  
**Optimized Performance with MapReduce:** Uses join operations and filtering to optimize computations  
**End-to-End Pipeline:** Covers **data preprocessing, transformation, and final computation**

## Running the Project
```bash
spark-submit --class Tri target/scala-2.12/triangle-counting.jar

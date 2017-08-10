package com.stalele.spark;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function0;

public class SparkOperation {
	
	/**
	 * @param String
	 */
	/**
	 * @param String
	 */
	/**
	 * @param String
	 */
	/**
	 * @param String
	 */
	public static void main(String[]String) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);		
		JavaSparkContext javaContext = SparkConnection.getSparkContext();
		//loading and storing data
		//create RDD from collection
		List<Integer> data = Arrays.asList(3,6,3,4,8, 10,12,34,10,5,6,7,9);
		JavaRDD<Integer> collectData = javaContext.parallelize(data);
		collectData.cache();
		
		//create RDD from a file
		JavaRDD<String> autoAllData = javaContext.textFile("data/auto-data.csv");
		System.out.println("Total records in " + autoAllData.count());
		printRDD(autoAllData);
	//autoAllData.saveAsTextFile("data/auto-data-modified.csv");
		List<String> autoList = autoAllData.collect();
		
		JavaRDD<String> irisRDD = javaContext.textFile("data/iris.csv");
		System.out.println("Total records in iris data file is" + irisRDD.count());
		//irisRDD.saveAsTextFile("data/iriris-modified-data.csv");
		
		System.out.println("Spark Map operation");
		printRDD(mapOperation(autoAllData));
		
		final String header =	autoAllData.first();
		JavaRDD<String> autoNewData = autoAllData.filter(s -> !s.equals(header));
		System.out.println("Spark Filter Header operation");
		printRDD(autoNewData);
		JavaRDD<String> autoToyotaData = autoAllData.filter(s -> s.contains("toyota"));
		System.out.println("Spark Filter Toyota cars operation");
		printRDD(autoToyotaData);
		
		System.out.println("Spark Filter Distinct operation " + collectData.distinct().collect());
		JavaRDD<String> words = autoToyotaData.flatMap(new FlatMapFunction<String, String>() {
			
			public Iterator<String> call(String s){
				return Arrays.asList(s.split(",")).iterator();
			}
		});
		printRDD(words);
		
		//filter iridRDD to get versicolor flowers
		JavaRDD<String> versiColorRDD = 	irisRDD.filter(s -> s.contains("versicolor"));
		System.out.println("Spark Filter versicolor operation ");
		printRDD(versiColorRDD);
		//convert numeric values into float use iris.csv
		String irisHeader =	irisRDD.first();
		JavaRDD<String> newIrisData = irisRDD.filter(s -> !s.equals(irisHeader));
		JavaRDD<String> newirisValues = newIrisData.map(new ConvertToFloat());
		System.out.println("Spark convert Sepal value to float operation");
		printRDD(newirisValues);
	}

	private static void printRDD(JavaRDD<String> autoAllData) {
		for(String s : autoAllData.take(5)) {
			System.out.println(s);
		}
	}
	
	public static JavaRDD mapOperation(JavaRDD<String> javaRdd) {
		return javaRdd.map(str -> str.replace(",","\t"));
	}
}

class ConvertToFloat implements Function<String,String> {
	@Override
	public String call(String flowerStr) throws Exception{
		String[] flowers = flowerStr.split(",");
		for(int i =0;i<flowers.length - 1;i++) {
			flowers[i] = String.valueOf((Math.round(Float.parseFloat(flowers[i]))));
		}
		
		//return Arrays.toString(flowers);
		//Concatenate array to string
		String result = flowers[0];
        for (int i=1; i<flowers.length; i++) {
            result = result + "," + flowers[i];
        }
		return result;
		
	}
}



package com.stalele.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkConnection {
	private static JavaSparkContext spContext;
	//private String tempDir = "file:"
	//a name for spark instance
	private static String appName = "spark experiment";
	//url to spark instance - embedded
	private static String sparkMaster = "local[2]";
	
	public static JavaSparkContext getSparkContext() {
		if(spContext == null) {
			//setup spark configuration
			SparkConf conf = new SparkConf()
					.setAppName(appName)
					.setMaster(sparkMaster);
			//create soarkcontext for configuration
			spContext = new JavaSparkContext(conf);
			
			return spContext;
			
		}
		return spContext;
		
	}

}

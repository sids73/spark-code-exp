package com.sid.bigdata.yellowtaxi;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
public class Application {

	public static void main(String[] args) {
		String runMode="prod";
		String hadoopWinDllPath="";
		String dataFilePath="/zelda-e-drive/Yellow_Taxi_Big_Data/data/";
		String outputResultsPath="/zelda-e-drive/";
		if(args.length>0) {
			runMode=args[0];
			hadoopWinDllPath=args[1];
			dataFilePath=args[2];
			outputResultsPath=args[3];
		}
			
		SparkSession spark= craftSparkSession(runMode,hadoopWinDllPath);
		
		Dataset<Row> ytDf = buildYellowTaxiDataFrame(spark,dataFilePath);
		//ytDf.printSchema();
		ytDf.repartition(3,col("compared_to_average_total_amount_by_vendor"));
		ytDf.write().mode("overwrite").partitionBy("compared_to_average_total_amount_by_vendor").csv(outputResultsPath);
		spark.close();

	}
	
	private static Dataset<Row> buildYellowTaxiDataFrame(SparkSession spark,String dataFilePath) {
		//yellow_tripdata_2009-*.csv
		Dataset<Row> df = spark.read().format("csv")
				.option("header", true)
				.load( dataFilePath);
		  df = df.withColumn("total_amount", df.col("Total_Amt").cast("int"));
		  df = df.groupBy("vendor_name").agg(sum("Total_Amt").alias("total_amount_by_vendor"));
		  
		  Dataset<Row> df_avg = df.select(avg("total_amount_by_vendor").alias("average_total_amount_by_vendor"));
		  df = df.crossJoin(df_avg);
		  df = df.withColumn("compared_to_average_total_amount_by_vendor", round(col("total_amount_by_vendor").divide(col("average_total_amount_by_vendor")),3));
	
		return df;
	}
    private static SparkSession craftSparkSession(String runMode, String hadoopWinDllPath) {
    	String osName = System.getProperty("os.name");
    	if (osName.contains("Windows")){
    		//System.load("E:/Hadoop_Home/bin/hadoop.dll");
    		System.load(hadoopWinDllPath);
    	}
    	SparkConf conf = new SparkConf();
		SparkSession spark = null;
		if(runMode.equalsIgnoreCase("develop")) {
			spark = SparkSession.builder()
			        .appName("NewYork_Yellow_Taxi_Data_Cruncher")
			        .master("local")
			        .getOrCreate();
		}
		else {
			spark = SparkSession.builder()
			        .config(conf)
			        .getOrCreate();
		}
		return spark;
    }
}

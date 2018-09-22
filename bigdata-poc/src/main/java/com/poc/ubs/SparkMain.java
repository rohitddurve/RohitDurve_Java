package com.poc.ubs;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.ColumnName;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

public class SparkMain {
	
	private static final long serialVersionUID = 6692184633779010628L;
	private static Logger logger = LoggerFactory.getLogger(SparkMain.class);
	
	private static String startOfDayInputFilePath = "C:\\Users\\Rohit\\Desktop\\Top_Urgent_Coding_Assignment\\Input_StartOfDay_Positions.txt";
	private static String transactionInputFilePath ="C:\\Users\\Rohit\\Desktop\\Top_Urgent_Coding_Assignment\\1537277231233_Input_Transactions.txt";
	private static String endOfDay_positionFilePath = "C:\\Users\\Rohit\\Desktop\\Top_Urgent_Coding_Assignment\\Expected_output";
	
	public static void main(String[] args) throws Exception {
		logger.info(">>Launching Spark Driver Program");
		
		if (System.getProperty("os.name").startsWith("Windows")) {
			System.setProperty("hadoop.home.dir", "D:/hadoop/hadoop-2.7.3");
		}
		
		SparkConf conf = new SparkConf().setAppName("POC").setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(jsc);
		sqlContext.sparkContext().setLogLevel("ERROR");
		sqlContext.sparkContext().hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
		
		//Reading Input_StartOfDay_Positions
		DataFrame startOfDayDataFrame= sqlContext
										.read()
										.format("com.databricks.spark.csv")
										.option("header", "true")
										.load(startOfDayInputFilePath);		
				
		//Reading 1537277231233_Input_Transactions
		JavaRDD<Tuple2<String, String>> inputTransaction = sqlContext.sparkContext()
				  											.wholeTextFiles(transactionInputFilePath, 1)
				  											.toJavaRDD();
		
		//Registering UDF
		TransactionHelper.registerUDF(sqlContext);
		
		//Converting string RDD into List<Row> RDD
		JavaRDD<List<Row>> transactionRDD = inputTransaction.map(new Function<Tuple2<String,String> , List<Row>>() {
			public List<Row> call(Tuple2<String,String> arg0) throws Exception {												
				List<Row> rows = TransactionHelper.readTransaction(arg0._2); 				   
				return rows;
			}			
		});
		
		//converting List<Row> RDD into Row RDD
		JavaRDD<Row> transactionRDD2 = transactionRDD.flatMap(new FlatMapFunction<List<Row>, Row>() {
			public Iterable<Row> call(List<Row> arg0) throws Exception {
				return arg0;
			}
		});
		
		//Creating Schema for Transaction DataFrame
		StructType transactionStructSchema = DataTypes.createStructType(TransactionHelper.getSchemaForTransaction());
		
		//Converting Transaction RDD into DataFrame
		DataFrame transactionDataFrame = sqlContext.createDataFrame(transactionRDD2, transactionStructSchema);
		
		//Registering as temo table		
		startOfDayDataFrame.registerTempTable("StartOfDayData");
		transactionDataFrame.registerTempTable("TranxTBL");		
		
		DataFrame txnDf = sqlContext.sql("select Instrument,TransactionType,Sum(TransactionQuantity) as SUM from TranxTBL group by Instrument,TransactionType");
		txnDf.registerTempTable("TranxTBLGRP");
		
		DataFrame txnDf123 = sqlContext.sql("select A.Instrument ,A.Account,A.AccountType,calculateTransactionB( A.AccountType, A.Quantity, C.Instrument, A.Instrument, C.TransactionType, C.SUM) as Qty from StartOfDayData A left join TranxTBLGRP C on A.Instrument = C.Instrument ");
		txnDf123.registerTempTable("Step1Table");
				
		DataFrame step1Output  = sqlContext.sql("select Instrument,Account,AccountType,sum(Qty) AS Qty from Step1Table  group by Instrument,Account,AccountType");
		step1Output.registerTempTable("Step1Output");
				
		DataFrame step2Input = sqlContext.sql("select A.Instrument,A.Account,A.AccountType,A.Qty,B.TransactionType,case when B.TransactionType = 'B' then 1 when B.TransactionType = 'S' then -1 end as BuySellFlag,B.SUM from Step1Output A left join TranxTBLGRP B ON A.Instrument=B.Instrument  group by A.Instrument,A.Account,A.AccountType,A.Qty,B.TransactionType,B.SUM");
		step2Input.registerTempTable("Step2Input");
				
		DataFrame step2Table = sqlContext.sql("select Instrument,Account,AccountType,calculateTransactionS( AccountType, cast(Qty as String), Instrument, TransactionType, SUM) as Qnty,Qty,BuySellFlag from Step2Input ");
		step2Table.registerTempTable("Step2Table");
				
		DataFrame step2Output = sqlContext.sql("select Instrument,Account,AccountType,case when sum(BuySellFlag) > 0 then Qty else sum(Qnty) end AS Qnty from Step2Table  group by Instrument,Account,AccountType,Qty");
		step2Output.registerTempTable("Step2Output");
		
		DataFrame finalOutput = sqlContext.sql("select Y.Instrument,Y.Account,Y.AccountType,cast(Y.Qnty as Bigint) as Quantity,cast((Y.Qnty-X.Quantity) as Bigint) as Delta from StartOfDayData X join Step2Output Y ON X.Account = Y.Account where X.Instrument = Y.Instrument");
		
		
		//Writing final output
		finalOutput
		.repartition(1)
		.write()		
		.format("com.databricks.spark.csv")
		.option("header", "true")		
		.save(endOfDay_positionFilePath);
		
		/* 
Input 1
+----------+-------+-----------+----------+
|Instrument|Account|AccountType|  Quantity|
+----------+-------+-----------+----------+
|      AMZN|    101|          E|    -10000|
|      AMZN|    201|          I|     10000|
|      APPL|    101|          E|     10000|
|      APPL|    201|          I|    -10000|
|       IBM|    101|          E|    100000|
|       IBM|    201|          I|   -100000|
|      MSFT|    201|          I|  -5000000|
|      MSFT|    101|          E|   5000000|
|      NFLX|    101|          E| 100000000|
|      NFLX|    201|          I|-100000000|
+----------+-------+-----------+----------+


Input 2

+-------------+----------+---------------+-------------------+
|TransactionId|Instrument|TransactionType|TransactionQuantity|
+-------------+----------+---------------+-------------------+
|            1|       IBM|              B|               1000|
|            2|      APPL|              S|                200|
|            3|      AMZN|              S|               5000|
|            4|      MSFT|              B|                 50|
|            5|      APPL|              B|                100|
|            6|      APPL|              S|              20000|
|            7|      AMZN|              S|               5000|
|            8|      MSFT|              S|                300|
|            9|      AMZN|              B|                200|
|           10|      APPL|              B|               9000|
|           11|      AMZN|              S|               5000|
|           12|      AMZN|              S|                 50|
+-------------+----------+---------------+-------------------+

 

Output
		 
+----------+-------+-----------+----------+--------+
|Instrument|Account|AccountType|  Quantity|   DELTA|
+----------+-------+-----------+----------+--------+
|      APPL|    101|          E|   -1100.0|-11100.0|
|      AMZN|    201|          I|   24850.0| 14850.0|
|      MSFT|    201|          I|-4999750.0|   250.0|
|      AMZN|    101|          E|  -24850.0|-14850.0|
|       IBM|    201|          I| -101000.0| -1000.0|
|      MSFT|    101|          E| 4999750.0|  -250.0|
|       IBM|    101|          E|  101000.0|  1000.0|
|      NFLX|    201|          I|    -1.0E8|     0.0|
|      NFLX|    101|          E|     1.0E8|     0.0|
|      APPL|    201|          I|    1100.0| 11100.0|
+----------+-------+-----------+----------+--------+
		 
		 */
		
	}

}

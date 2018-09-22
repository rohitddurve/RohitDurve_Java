package com.poc.ubs;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class TransactionHelper {
	
	public static String[] trancastionSchema = new String[]{"TransactionId","Instrument","TransactionType","TransactionQuantity"};

	//Reading JSON Data into List<Row>
    public static List<Row> readTransaction(String str) {
    	    	
        JSONParser parser = new JSONParser();
        JSONObject jobject = null;
        List<Row> rowList = new ArrayList<Row>();

        try {
        	
        	
             org.json.simple.JSONArray jsonArray= (org.json.simple.JSONArray) parser.parse(str);
             
             for (Object Object : jsonArray) {			
			
            	 jobject = (JSONObject)Object;
            	 
            	 String[] tranValues =  new String[jobject.keySet().size()];		
				   
				    tranValues[0]= String.valueOf(jobject.get(TransactionHelper.trancastionSchema[0]));
				    tranValues[1]= String.valueOf(jobject.get(TransactionHelper.trancastionSchema[1]));
				    tranValues[2] = String.valueOf(jobject.get(TransactionHelper.trancastionSchema[2]));
				    tranValues[3] = String.valueOf(jobject.get(TransactionHelper.trancastionSchema[3]));
				    Row r = RowFactory.create(tranValues);
				    rowList.add(r);  
            	
             }        
        } catch (Exception e) {
            e.printStackTrace();
        }
		return rowList;

    }
    
    
    public static List<StructField> getSchemaForTransaction(){
    	
    	List<StructField> transactionfields = new ArrayList<StructField>();		
		StructField tfield1 = DataTypes.createStructField(TransactionHelper.trancastionSchema[0], DataTypes.StringType, true);
		StructField tfield2 = DataTypes.createStructField(TransactionHelper.trancastionSchema[1], DataTypes.StringType, true);
		StructField tfield3 = DataTypes.createStructField(TransactionHelper.trancastionSchema[2], DataTypes.StringType, true);
		StructField tfield4 = DataTypes.createStructField(TransactionHelper.trancastionSchema[3], DataTypes.StringType, true);
		transactionfields.add(tfield1);
		transactionfields.add(tfield2);
		transactionfields.add(tfield3);
		transactionfields.add(tfield4);
		
		return transactionfields;
    }
    
    public static void registerUDF(SQLContext sqlContext){
    	
    	CalculateTransactionBUDF calculateTransactionBUDF = new CalculateTransactionBUDF();
		sqlContext.udf().register("calculateTransactionB", calculateTransactionBUDF, DataTypes.DoubleType);
		
		CalculateTransactionSUDF calculateTransactionSUDF = new CalculateTransactionSUDF();
		sqlContext.udf().register("calculateTransactionS", calculateTransactionSUDF, DataTypes.DoubleType);
    }

}
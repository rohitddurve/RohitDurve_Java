package com.poc.ubs;

import org.apache.spark.sql.api.java.UDF5;

public class CalculateTransactionSUDF implements UDF5<String, String, String, String,Double,Double> {
	private static final long serialVersionUID = 1L;
	
	
	
	public Double call( String accountType, String quantity, String instrument,
			String transactionType, Double sum) throws Exception {
		// TODO Auto-generated method stub
		Double outputQty = 0.0;
		if(transactionType!= null){
			 
			 if(transactionType.equals("S")){
				 
				 
				if(accountType.equals("E")){
					outputQty = Double.valueOf(quantity) - sum;
				}else if(accountType.equals("I")){
					outputQty = Double.valueOf(quantity) + sum;
				}
			 }
			
				
			
		}else{
			outputQty = Double.valueOf(quantity);
		}
		return outputQty;
	}
}

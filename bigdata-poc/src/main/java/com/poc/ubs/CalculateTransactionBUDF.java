package com.poc.ubs;

import org.apache.spark.sql.api.java.UDF6;

public class CalculateTransactionBUDF implements UDF6<String, String,String, String, String,Double,Double> {
	private static final long serialVersionUID = 1L;
	
	
	
	public Double call( String accountType, String quantity, String instrumentA,
			String instrumentC,String transactionTypeC, Double sum) throws Exception {
		// TODO Auto-generated method stub
		Double outputQty = 0.0;
		if(instrumentA!= null && instrumentA.equals(instrumentC)){
			
				if(transactionTypeC.equals("B")){			
					if(accountType.equals("E")){
						outputQty = Double.valueOf(quantity) + sum;
					}else if(accountType.equals("I")){
						outputQty = Double.valueOf(quantity) - sum;
					}
				}
				
			
		}else{
			outputQty = Double.valueOf(quantity);
		}
		return outputQty;
	}
}

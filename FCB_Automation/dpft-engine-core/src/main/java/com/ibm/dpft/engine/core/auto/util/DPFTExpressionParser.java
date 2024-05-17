package com.ibm.dpft.engine.core.auto.util;

import com.ibm.dpft.engine.core.exception.DPFTAutomationException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public class DPFTExpressionParser {
	private static final String[] operators = { "!=", "==", ">=", "<=", ">", "<"};

	public boolean evaluate(String condition) throws DPFTRuntimeException {
		String expression = condition;
		String op = getOperator(expression);
		if(op == null){
			StringBuilder sb = new StringBuilder();
			for(String o: operators){
				sb.append(o).append(",");
			}
			Object[] params = {sb.substring(0, sb.length()-1)};
			throw new DPFTAutomationException("SYSTEM", "AUTO0024E", params);
		}
		String[] operants = expression.split(op);
		int arg1 = Integer.valueOf(operants[0].trim());
		int arg2 = Integer.valueOf(operants[1].trim());
		switch(op){
		case ">":  return arg1 > arg2;
		case "<":  return arg1 < arg2;
		case ">=": return arg1 >= arg2;
		case "<=": return arg1 <= arg2;
		case "==": return arg1 == arg2;
		case "!=": return arg1 != arg2;
		default:
			return false;
		}
	}

	private String getOperator(String expression) {
		for(String op: operators){
			if(expression.indexOf(op) != -1){
				return op;
			}
		}
		return null;
	}

}

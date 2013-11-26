package com.buysidefx.exceptions;

public class DatabaseException extends Exception {
	Exception e;
	public DatabaseException(String reason, Exception e) {
		super(reason);
		if (e!=null) {
			this.e = e;
			super.addSuppressed(e);
		}
		
	}
	
	public String toString() {
		String s = super.toString()+"\n";
		StackTraceElement[] e = this.getStackTrace();
		for (int i=0;i<e.length;i++) {
			s+=(e[i].toString()+"\n");
		}
		return s;
	}
	
}

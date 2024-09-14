package io.odysz.semantic.DA.drvmnger;

import java.sql.Statement;
import java.util.concurrent.locks.ReentrantLock;

public class StatementOnCall {

	@FunctionalInterface
	public interface OnCommit {
		void ok(int[] res);
	}

	Statement statment;
	OnCommit onCommit;

	// ReentrantLock lock;
	Object lock;
	public boolean finished;

	public StatementOnCall(Statement stmt, OnCommit on) {
		this.statment = stmt;
		this.onCommit = on;
		
		// lock = new ReentrantLock();
		lock = new Object();
		finished = false;
	}

//	public StatementOnCall lock() {
//		lock.lock();
//		return this;
//	}
//
//	public void unlock() {
//		lock.unlock();
//	}
}

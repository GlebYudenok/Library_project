package dao.mysql;

import java.sql.Connection;
import java.sql.SQLException;

import dao.pool.ConnectionPool;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dao.Transaction;
import dao.TransactionFactory;
import exception.PersistentException;

public class TransactionFactoryImpl implements TransactionFactory {
	private static Logger logger = LogManager.getLogger(TransactionFactoryImpl.class);
	private Connection connection;
	
	public TransactionFactoryImpl() throws PersistentException {
		connection = ConnectionPool.getInstance().getConnection();
		try {
			connection.setAutoCommit(false);
		} catch(SQLException e) {
			logger.error("It is impossible to turn off autocommiting for database connection", e);
			throw new PersistentException(e);
		}
	}

	@Override
	public Transaction createTransaction() throws PersistentException {
		return new TransactionImpl(connection);
	}

	@Override
	public void close() {
		try {
			connection.close();
		} catch(SQLException e) {}
	}
}

package dao.mysql;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import dao.UsageDao;
import domain.Book;
import domain.Reader;
import domain.Usage;
import exception.PersistentException;

public class UsageDaoImpl extends BaseDaoImpl implements UsageDao {
	private static Logger logger = LogManager.getLogger(UsageDaoImpl.class);

	@Override
	public Integer create(Usage usage) throws PersistentException {
		String sql = "INSERT INTO `usages` (`book_identity`, `reader_identity`, `delivery_date`, `return_date`, `plan_return_date`) VALUES (?, ?, ?, ?, ?)";
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			statement.setInt(1, usage.getBook().getIdentity());
			statement.setInt(2, usage.getReader().getIdentity());
			statement.setDate(3, new Date(usage.getDeliveryDate().getTime()));
			if(usage.getReturnDate() != null) {
				statement.setDate(4, new Date(usage.getReturnDate().getTime()));
			} else {
				statement.setNull(4, Types.DATE);
			}
			statement.setDate(5, new Date(usage.getPlanReturnDate().getTime()));
			statement.executeUpdate();
			resultSet = statement.getGeneratedKeys();
			if(resultSet.next()) {
				return resultSet.getInt(1);
			} else {
				logger.error("There is no autoincremented index after trying to add record into table `usages`");
				throw new PersistentException();
			}
		} catch(SQLException e) {
			throw new PersistentException(e);
		} finally {
			try {
				resultSet.close();
			} catch(SQLException | NullPointerException e) {}
			try {
				statement.close();
			} catch(SQLException | NullPointerException e) {}
		}
	}

	@Override
	public Usage read(Integer identity) throws PersistentException {
		String sql = "SELECT `book_identity`, `reader_identity`, `delivery_date`, `return_date`, `plan_return_date` FROM `usages` WHERE `identity` = ?";
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = connection.prepareStatement(sql);
			statement.setInt(1, identity);
			resultSet = statement.executeQuery();
			Usage usage = null;
			if(resultSet.next()) {
				usage = new Usage();
				usage.setIdentity(identity);
				Book book = new Book();
				book.setIdentity(resultSet.getInt("book_identity"));
				usage.setBook(book);
				Reader reader = new Reader();
				reader.setIdentity(resultSet.getInt("reader_identity"));
				usage.setReader(reader);
				usage.setDeliveryDate(new java.util.Date(resultSet.getDate("delivery_date").getTime()));
				Date returnDate = resultSet.getDate("return_date");
				if(!resultSet.wasNull()) {
					usage.setReturnDate(new java.util.Date(returnDate.getTime()));
				}
				usage.setPlanReturnDate(new java.util.Date(resultSet.getDate("plan_return_date").getTime()));
			}
			return usage;
		} catch(SQLException e) {
			throw new PersistentException(e);
		} finally {
			try {
				resultSet.close();
			} catch(SQLException | NullPointerException e) {}
			try {
				statement.close();
			} catch(SQLException | NullPointerException e) {}
		}
	}

	@Override
	public void update(Usage usage) throws PersistentException {
		String sql = "UPDATE `usages` SET `book_identity` = ?, `reader_identity` = ?, `delivery_date` = ?, `return_date` = ?, `plan_return_date` = ? WHERE `identity` = ?";
		PreparedStatement statement = null;
		try {
			statement = connection.prepareStatement(sql);
			statement.setInt(1, usage.getBook().getIdentity());
			statement.setInt(2, usage.getReader().getIdentity());
			statement.setDate(3, new Date(usage.getDeliveryDate().getTime()));
			if(usage.getReturnDate() != null) {
				statement.setDate(4, new Date(usage.getReturnDate().getTime()));
			} else {
				statement.setNull(4, Types.DATE);
			}
			statement.setDate(5, new Date(usage.getPlanReturnDate().getTime()));
			statement.setInt(6, usage.getIdentity());
			statement.executeUpdate();
		} catch(SQLException e) {
			throw new PersistentException(e);
		} finally {
			try {
				statement.close();
			} catch(SQLException | NullPointerException e) {}
		}
	}

	@Override
	public void delete(Integer identity) throws PersistentException {
		String sql = "DELETE FROM `usages` WHERE `identity` = ?";
		PreparedStatement statement = null;
		try {
			statement = connection.prepareStatement(sql);
			statement.setInt(1, identity);
			statement.executeUpdate();
		} catch(SQLException e) {
			throw new PersistentException(e);
		} finally {
			try {
				statement.close();
			} catch(SQLException | NullPointerException e) {}
		}
	}

	@Override
	public List<Usage> readByReader(Integer readerIdentity) throws PersistentException {
		String sql = "SELECT `identity`, `book_identity`, `delivery_date`, `return_date`, `plan_return_date` FROM `usages` WHERE `reader_identity` = ? ORDER BY `delivery_date` DESC";
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = connection.prepareStatement(sql);
			statement.setInt(1, readerIdentity);
			resultSet = statement.executeQuery();
			List<Usage> usages = new ArrayList<>();
			Usage usage = null;
			Reader reader = new Reader();
			reader.setIdentity(readerIdentity);
			while(resultSet.next()) {
				usage = new Usage();
				usage.setIdentity(resultSet.getInt("identity"));
				Book book = new Book();
				book.setIdentity(resultSet.getInt("book_identity"));
				usage.setBook(book);
				usage.setReader(reader);
				usage.setDeliveryDate(new java.util.Date(resultSet.getDate("delivery_date").getTime()));
				Date returnDate = resultSet.getDate("return_date");
				if(!resultSet.wasNull()) {
					usage.setReturnDate(new java.util.Date(returnDate.getTime()));
				}
				usage.setPlanReturnDate(new java.util.Date(resultSet.getDate("plan_return_date").getTime()));
				usages.add(usage);
			}
			return usages;
		} catch(SQLException e) {
			throw new PersistentException(e);
		} finally {
			try {
				resultSet.close();
			} catch(SQLException | NullPointerException e) {}
			try {
				statement.close();
			} catch(SQLException | NullPointerException e) {}
		}
	}

	@Override
	public List<Usage> readOverdue() throws PersistentException {
		String sql = "SELECT `identity`, `book_identity`, `reader_identity`, `delivery_date`, `plan_return_date` FROM `usages` WHERE `return_date` IS NULL AND DATEDIFF(CURDATE(), `plan_return_date`) > 0";
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = connection.prepareStatement(sql);
			resultSet = statement.executeQuery();
			List<Usage> usages = new ArrayList<>();
			Usage usage = null;
			while(resultSet.next()) {
				usage = new Usage();
				usage.setIdentity(resultSet.getInt("identity"));
				Book book = new Book();
				book.setIdentity(resultSet.getInt("book_identity"));
				usage.setBook(book);
				Reader reader = new Reader();
				reader.setIdentity(resultSet.getInt("reader_identity"));
				usage.setReader(reader);
				usage.setDeliveryDate(new java.util.Date(resultSet.getDate("delivery_date").getTime()));
				usage.setPlanReturnDate(new java.util.Date(resultSet.getDate("plan_return_date").getTime()));
				usages.add(usage);
			}
			return usages;
		} catch(SQLException e) {
			throw new PersistentException(e);
		} finally {
			try {
				resultSet.close();
			} catch(SQLException | NullPointerException e) {}
			try {
				statement.close();
			} catch(SQLException | NullPointerException e) {}
		}
	}

	@Override
	public List<Usage> readByBook(Integer bookIdentity) throws PersistentException {
		String sql = "SELECT `identity`, `reader_identity`, `delivery_date`, `return_date`, `plan_return_date` FROM `usages` WHERE `book_identity` = ? ORDER BY `delivery_date`";
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = connection.prepareStatement(sql);
			statement.setInt(1, bookIdentity);
			resultSet = statement.executeQuery();
			List<Usage> usages = new ArrayList<>();
			Usage usage = null;
			Book book = new Book();
			book.setIdentity(bookIdentity);
			while(resultSet.next()) {
				usage = new Usage();
				usage.setIdentity(resultSet.getInt("identity"));
				usage.setBook(book);
				Reader reader = new Reader();
				reader.setIdentity(resultSet.getInt("reader_identity"));
				usage.setReader(reader);
				usage.setDeliveryDate(new java.util.Date(resultSet.getDate("delivery_date").getTime()));
				Date returnDate = resultSet.getDate("return_date");
				if(!resultSet.wasNull()) {
					usage.setReturnDate(new java.util.Date(returnDate.getTime()));
				}
				usage.setPlanReturnDate(new java.util.Date(resultSet.getDate("plan_return_date").getTime()));
				usages.add(usage);
			}
			return usages;
		} catch(SQLException e) {
			throw new PersistentException(e);
		} finally {
			try {
				resultSet.close();
			} catch(SQLException | NullPointerException e) {}
			try {
				statement.close();
			} catch(SQLException | NullPointerException e) {}
		}
	}
}

package dao.mysql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dao.BookDao;
import domain.Author;
import domain.Book;
import exception.PersistentException;

public class BookDaoImpl extends BaseDaoImpl implements BookDao {
	private static Logger logger = LogManager.getLogger(BookDaoImpl.class);

	@Override
	public Integer create(Book book) throws PersistentException {
		String sql = "INSERT INTO `books` (`inventory_number`, `title`, `author_identity`, `year`) VALUES (?, ?, ?, ?)";
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			statement.setString(1, book.getInventoryNumber());
			statement.setString(2, book.getTitle());
			if(book.getAuthor() != null && book.getAuthor().getIdentity() != null) {
				statement.setInt(3, book.getAuthor().getIdentity());
			} else {
				statement.setNull(3, Types.INTEGER);
			}
			statement.setInt(4, book.getYear());
			statement.executeUpdate();
			resultSet = statement.getGeneratedKeys();
			if(resultSet.next()) {
				return resultSet.getInt(1);
			} else {
				logger.error("There is no autoincremented index after trying to add record into table `books`");
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
	public Book read(Integer identity) throws PersistentException {
		String sql = "SELECT `inventory_number`, `title`, `author_identity`, `year` FROM `books` WHERE `identity` = ?";
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = connection.prepareStatement(sql);
			statement.setInt(1, identity);
			resultSet = statement.executeQuery();
			Book book = null;
			if(resultSet.next()) {
				book = new Book();
				book.setIdentity(identity);
				book.setInventoryNumber(resultSet.getString("inventory_number"));
				book.setTitle(resultSet.getString("title"));
				Integer authorIdentity = resultSet.getInt("author_identity");
				if(!resultSet.wasNull()) {
					Author author = new Author();
					author.setIdentity(authorIdentity);
					book.setAuthor(author);
				}
				book.setYear(resultSet.getInt("year"));
			}
			return book;
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
	public void update(Book book) throws PersistentException {
		String sql = "UPDATE `books` SET `inventory_number` = ?, `title` = ?, `author_identity` = ?, `year` = ? WHERE `identity` = ?";
		PreparedStatement statement = null;
		try {
			statement = connection.prepareStatement(sql);
			statement.setString(1, book.getInventoryNumber());
			statement.setString(2, book.getTitle());
			if(book.getAuthor() != null && book.getAuthor().getIdentity() != null) {
				statement.setInt(3, book.getAuthor().getIdentity());
			} else {
				statement.setNull(3, Types.INTEGER);
			}
			statement.setInt(4, book.getYear());
			statement.setInt(5, book.getIdentity());
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
		String sql = "DELETE FROM `books` WHERE `identity` = ?";
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
	public List<Book> readByAuthor(Integer authorIdentity) throws PersistentException {
		String sql;
		if(authorIdentity != null) {
			sql = "SELECT `identity`, `inventory_number`, `title`, `year` FROM `books` WHERE `author_identity` = ? ORDER BY `title`";
		} else {
			sql = "SELECT `identity`, `inventory_number`, `title`, `year` FROM `books` WHERE `author_identity` IS NULL ORDER BY `title`";
		}
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = connection.prepareStatement(sql);
			if(authorIdentity != null) {
				statement.setInt(1, authorIdentity);
			}
			resultSet = statement.executeQuery();
			List<Book> books = new ArrayList<>();
			Book book = null;
			Author author = null;
			if(authorIdentity != null) {
				author = new Author();
				author.setIdentity(authorIdentity);
			}
			while(resultSet.next()) {
				book = new Book();
				book.setIdentity(resultSet.getInt("identity"));
				book.setInventoryNumber(resultSet.getString("inventory_number"));
				book.setTitle(resultSet.getString("title"));
				book.setAuthor(author);
				book.setYear(resultSet.getInt("year"));
				books.add(book);
			}
			return books;
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
	public List<Book> readByTitle(String search) throws PersistentException {
		String sql = "SELECT `identity`, `inventory_number`, `title`, `author_identity`, `year` FROM `books` WHERE `title` LIKE ? ORDER BY `title`";
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = connection.prepareStatement(sql);
			statement.setString(1, "%" + search + "%");
			resultSet = statement.executeQuery();
			List<Book> books = new ArrayList<>();
			Book book = null;
			while(resultSet.next()) {
				book = new Book();
				book.setIdentity(resultSet.getInt("identity"));
				book.setInventoryNumber(resultSet.getString("inventory_number"));
				book.setTitle(resultSet.getString("title"));
				Author author = new Author();
				author.setIdentity(resultSet.getInt("author_identity"));
				if(!resultSet.wasNull()) {
					book.setAuthor(author);
				}
				book.setYear(resultSet.getInt("year"));
				books.add(book);
			}
			return books;
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
	public Book readByInventoryNumber(String inventoryNumber) throws PersistentException {
		String sql = "SELECT `identity`, `title`, `author_identity`, `year` FROM `books` WHERE `inventory_number` = ?";
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = connection.prepareStatement(sql);
			statement.setString(1, inventoryNumber);
			resultSet = statement.executeQuery();
			Book book = null;
			if(resultSet.next()) {
				book = new Book();
				book.setIdentity(resultSet.getInt("identity"));
				book.setInventoryNumber(inventoryNumber);
				book.setTitle(resultSet.getString("title"));
				Integer authorIdentity = resultSet.getInt("author_identity");
				if(!resultSet.wasNull()) {
					Author author = new Author();
					author.setIdentity(authorIdentity);
					book.setAuthor(author);
				}
				book.setYear(resultSet.getInt("year"));
			}
			return book;
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

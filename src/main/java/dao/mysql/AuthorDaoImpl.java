package dao.mysql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dao.AuthorDao;
import domain.Author;
import exception.PersistentException;

public class AuthorDaoImpl extends BaseDaoImpl implements AuthorDao {
	private static Logger logger = LogManager.getLogger(AuthorDaoImpl.class);

	@Override
	public Integer create(Author author) throws PersistentException {
		String sql = "INSERT INTO `authors` (`surname`, `name`, `patronymic`, `year_of_birth`, `year_of_death`) VALUES (?, ?, ?, ?, ?)";
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			statement.setString(1, author.getSurname());
			statement.setString(2, author.getName());
			statement.setString(3, author.getPatronymic());
			statement.setInt(4, author.getYearOfBirth());
			if(author.getYearOfDeath() != null) {
				statement.setInt(5, author.getYearOfDeath());
			} else {
				statement.setNull(5, Types.INTEGER);
			}
			statement.executeUpdate();
			resultSet = statement.getGeneratedKeys();
			if(resultSet.next()) {
				return resultSet.getInt(1);
			} else {
				logger.error("There is no autoincremented index after trying to add record into table `authors`");
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
	public Author read(Integer identity) throws PersistentException {
		String sql = "SELECT `surname`, `name`, `patronymic`, `year_of_birth`, `year_of_death` FROM `authors` WHERE `identity` = ?";
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = connection.prepareStatement(sql);
			statement.setInt(1, identity);
			resultSet = statement.executeQuery();
			Author author = null;
			if(resultSet.next()) {
				author = new Author();
				author.setIdentity(identity);
				author.setSurname(resultSet.getString("surname"));
				author.setName(resultSet.getString("name"));
				author.setPatronymic(resultSet.getString("patronymic"));
				author.setYearOfBirth(resultSet.getInt("year_of_birth"));
				Integer yearOfDeath = resultSet.getInt("year_of_death");
				if(!resultSet.wasNull()) {
					author.setYearOfDeath(yearOfDeath);
				}
			}
			return author;
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
	public void update(Author author) throws PersistentException {
		String sql = "UPDATE `authors` SET `surname` = ?, `name` = ?, `patronymic` = ?, `year_of_birth` = ?, `year_of_death` = ? WHERE `identity` = ?";
		PreparedStatement statement = null;
		try {
			statement = connection.prepareStatement(sql);
			statement.setString(1, author.getSurname());
			statement.setString(2, author.getName());
			statement.setString(3, author.getPatronymic());
			statement.setInt(4, author.getYearOfBirth());
			if(author.getYearOfDeath() != null) {
				statement.setInt(5, author.getYearOfDeath());
			} else {
				statement.setNull(5, Types.INTEGER);
			}
			statement.setInt(6, author.getIdentity());
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
		String sql = "DELETE FROM `authors` WHERE `identity` = ?";
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
	public List<Author> read() throws PersistentException {
		String sql = "SELECT `identity`, `surname`, `name`, `patronymic`, `year_of_birth`, `year_of_death` FROM `authors` ORDER BY `surname`, `name`, `patronymic`, `year_of_birth`";
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = connection.prepareStatement(sql);
			resultSet = statement.executeQuery();
			List<Author> authors = new ArrayList<>();
			Author author = null;
			while(resultSet.next()) {
				author = new Author();
				author.setIdentity(resultSet.getInt("identity"));
				author.setSurname(resultSet.getString("surname"));
				author.setName(resultSet.getString("name"));
				author.setPatronymic(resultSet.getString("patronymic"));
				author.setYearOfBirth(resultSet.getInt("year_of_birth"));
				Integer yearOfDeath = resultSet.getInt("year_of_death");
				if(!resultSet.wasNull()) {
					author.setYearOfDeath(yearOfDeath);
				}
				authors.add(author);
			}
			return authors;
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
	public Map<Author, Integer> readWithNumberOfBooks() throws PersistentException {
		String sql = "SELECT `authors`.`identity`, `authors`.`surname`, `authors`.`name`, `authors`.`patronymic`, `authors`.`year_of_birth`, `authors`.`year_of_death`, COUNT(`books`.`identity`) AS `number_of_books` FROM `authors` LEFT JOIN `books` ON `authors`.`identity` = `books`.`author_identity` WHERE `books`.`identity` NOT IN (SELECT `usages`.`book_identity` FROM `usages` WHERE `usages`.`return_date` IS NULL) GROUP BY `authors`.`identity`, `authors`.`surname`, `authors`.`name`, `authors`.`patronymic`, `authors`.`year_of_birth`, `authors`.`year_of_death` ORDER BY `authors`.`surname`, `authors`.`name`, `authors`.`patronymic`, `authors`.`year_of_birth`";
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = connection.prepareStatement(sql);
			resultSet = statement.executeQuery();
			Map<Author, Integer> authors = new LinkedHashMap<>();
			Author author = null;
			while(resultSet.next()) {
				author = new Author();
				author.setIdentity(resultSet.getInt("identity"));
				author.setSurname(resultSet.getString("surname"));
				author.setName(resultSet.getString("name"));
				author.setPatronymic(resultSet.getString("patronymic"));
				author.setYearOfBirth(resultSet.getInt("year_of_birth"));
				Integer yearOfDeath = resultSet.getInt("year_of_death");
				if(!resultSet.wasNull()) {
					author.setYearOfDeath(yearOfDeath);
				}
				authors.put(author, resultSet.getInt("number_of_books"));
			}
			return authors;
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

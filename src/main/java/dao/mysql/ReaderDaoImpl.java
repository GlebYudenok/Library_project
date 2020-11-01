package dao.mysql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import dao.ReaderDao;
import domain.Reader;
import exception.PersistentException;

public class ReaderDaoImpl extends BaseDaoImpl implements ReaderDao {
	private static Logger logger = LogManager.getLogger(ReaderDaoImpl.class);

	@Override
	public Integer create(Reader reader) throws PersistentException {
		String sql = "INSERT INTO `readers` (`surname`, `name`, `patronymic`, `library_card_number`, `address`, `phone`) VALUES (?, ?, ?, ?, ?, ?)";
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			statement.setString(1, reader.getSurname());
			statement.setString(2, reader.getName());
			statement.setString(3, reader.getPatronymic());
			statement.setString(4, reader.getLibraryCardNumber());
			statement.setString(5, reader.getAddress());
			statement.setString(6, reader.getPhone());
			statement.executeUpdate();
			resultSet = statement.getGeneratedKeys();
			if(resultSet.next()) {
				return resultSet.getInt(1);
			} else {
				logger.error("There is no autoincremented index after trying to add record into table `readers`");
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
	public Reader read(Integer identity) throws PersistentException {
		String sql = "SELECT `surname`, `name`, `patronymic`, `library_card_number`, `address`, `phone` FROM `readers` WHERE `identity` = ?";
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = connection.prepareStatement(sql);
			statement.setInt(1, identity);
			resultSet = statement.executeQuery();
			Reader reader = null;
			if(resultSet.next()) {
				reader = new Reader();
				reader.setIdentity(identity);
				reader.setSurname(resultSet.getString("surname"));
				reader.setName(resultSet.getString("name"));
				reader.setPatronymic(resultSet.getString("patronymic"));
				reader.setLibraryCardNumber(resultSet.getString("library_card_number"));
				reader.setAddress(resultSet.getString("address"));
				reader.setPhone(resultSet.getString("phone"));
			}
			return reader;
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
	public void update(Reader reader) throws PersistentException {
		String sql = "UPDATE `readers` SET `surname` = ?, `name` = ?, `patronymic` = ?, `library_card_number` = ?, `address` = ?, `phone` = ? WHERE `identity` = ?";
		PreparedStatement statement = null;
		try {
			statement = connection.prepareStatement(sql);
			statement.setString(1, reader.getSurname());
			statement.setString(2, reader.getName());
			statement.setString(3, reader.getPatronymic());
			statement.setString(4, reader.getLibraryCardNumber());
			statement.setString(5, reader.getAddress());
			statement.setString(6, reader.getPhone());
			statement.setInt(7, reader.getIdentity());
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
		String sql = "DELETE FROM `readers` WHERE `identity` = ?";
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
	public List<Reader> read() throws PersistentException {
		String sql = "SELECT `identity`, `surname`, `name`, `patronymic`, `library_card_number`, `address`, `phone` FROM `readers` ORDER BY `surname`, `name`, `patronymic`";
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = connection.prepareStatement(sql);
			resultSet = statement.executeQuery();
			List<Reader> readers = new ArrayList<>();
			Reader reader = null;
			while(resultSet.next()) {
				reader = new Reader();
				reader.setIdentity(resultSet.getInt("identity"));
				reader.setSurname(resultSet.getString("surname"));
				reader.setName(resultSet.getString("name"));
				reader.setPatronymic(resultSet.getString("patronymic"));
				reader.setLibraryCardNumber(resultSet.getString("library_card_number"));
				reader.setAddress(resultSet.getString("address"));
				reader.setPhone(resultSet.getString("phone"));
				readers.add(reader);
			}
			return readers;
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
	public List<Reader> read(String search) throws PersistentException {
		String sql = "SELECT `identity`, `surname`, `name`, `patronymic`, `library_card_number`, `address`, `phone` FROM `readers` WHERE `surname` LIKE ? ORDER BY `surname`, `name`, `patronymic`";
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = connection.prepareStatement(sql);
			statement.setString(1, "%" + search + "%");
			resultSet = statement.executeQuery();
			List<Reader> readers = new ArrayList<>();
			Reader reader = null;
			while(resultSet.next()) {
				reader = new Reader();
				reader.setIdentity(resultSet.getInt("identity"));
				reader.setSurname(resultSet.getString("surname"));
				reader.setName(resultSet.getString("name"));
				reader.setPatronymic(resultSet.getString("patronymic"));
				reader.setLibraryCardNumber(resultSet.getString("library_card_number"));
				reader.setAddress(resultSet.getString("address"));
				reader.setPhone(resultSet.getString("phone"));
				readers.add(reader);
			}
			return readers;
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
	public Reader readByLibraryCardNumber(String libraryCardNumber) throws PersistentException {
		String sql = "SELECT `identity`, `surname`, `name`, `patronymic`, `address`, `phone` FROM `readers` WHERE `library_card_number` = ?";
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = connection.prepareStatement(sql);
			statement.setString(1, libraryCardNumber);
			resultSet = statement.executeQuery();
			Reader reader = null;
			if(resultSet.next()) {
				reader = new Reader();
				reader.setIdentity(resultSet.getInt("identity"));
				reader.setSurname(resultSet.getString("surname"));
				reader.setName(resultSet.getString("name"));
				reader.setPatronymic(resultSet.getString("patronymic"));
				reader.setLibraryCardNumber(libraryCardNumber);
				reader.setAddress(resultSet.getString("address"));
				reader.setPhone(resultSet.getString("phone"));
			}
			return reader;
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

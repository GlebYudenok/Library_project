package dao;

import java.util.List;
import java.util.Map;

import domain.Author;
import exception.PersistentException;

public interface AuthorDao extends Dao<Author> {
	List<Author> read() throws PersistentException;

	Map<Author, Integer> readWithNumberOfBooks() throws PersistentException;
}

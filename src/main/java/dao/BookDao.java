package dao;

import java.util.List;

import domain.Book;
import exception.PersistentException;

public interface BookDao extends Dao<Book> {
	Book readByInventoryNumber(String inventoryNumber) throws PersistentException;

	List<Book> readByAuthor(Integer authorIdentity) throws PersistentException;

	List<Book> readByTitle(String search) throws PersistentException;
}

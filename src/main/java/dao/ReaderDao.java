package dao;

import java.util.List;

import domain.Reader;
import exception.PersistentException;

public interface ReaderDao extends Dao<Reader> {
	Reader readByLibraryCardNumber(String libraryCardNumber) throws PersistentException;

	List<Reader> read() throws PersistentException;

	List<Reader> read(String search) throws PersistentException;
}

package dao;

import java.util.List;

import domain.Usage;
import exception.PersistentException;

public interface UsageDao extends Dao<Usage> {
	List<Usage> readByReader(Integer readerIdentity) throws PersistentException;

	List<Usage> readByBook(Integer bookIdentity) throws PersistentException;

	List<Usage> readOverdue() throws PersistentException;
}

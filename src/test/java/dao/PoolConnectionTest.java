package dao;

import dao.mysql.BookDaoImpl;
import dao.pool.ConnectionPool;
import exception.PersistentException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;

public class PoolConnectionTest {

    @Before
    public void setUp() throws PersistentException {

    }

    @Test
    public void create() throws PersistentException, SQLException {
        ConnectionPool cp = ConnectionPool.getInstance();
        Logger logger = LogManager.getLogger(PoolConnectionTest.class);
        BookDao bd = new BookDaoImpl();
        bd.readByAuthor(1);
    }
}

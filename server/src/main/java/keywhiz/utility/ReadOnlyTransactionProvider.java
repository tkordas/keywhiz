package keywhiz.utility;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import org.jooq.ConnectionProvider;
import org.jooq.TransactionContext;
import org.jooq.TransactionProvider;
import org.jooq.exception.DataAccessException;

/**
 * A jOOQ TransactionProvider designed for read-only classes in mind,
 * see https://github.com/jOOQ/jOOQ/issues/3955 for more info.
 *
 * TODO: write some tests, then write some more tests and finally write tests!
 */
public class ReadOnlyTransactionProvider  implements TransactionProvider {
  private final ConnectionProvider provider;

  public ReadOnlyTransactionProvider(ConnectionProvider provider) {
    this.provider = provider;
  }

  @Override
  public final void begin(TransactionContext ctx) {
    Map<Object, Object> data = ctx.configuration().data();
    int counter = (int)data.compute("counter", (k, v) -> (v == null) ? 1 : (int) v + 1);
    if (counter > 1) {
      // We are in a sub-transaction, don't do anything.
      return;
    }
    try {
      Connection connection = provider.acquire();
      if (data.get("connection") != null) {
        throw new DataAccessException("begin failed: connection wasn't null");
      }
      if (!connection.getAutoCommit()) {
        // If auto-commit was false, how do we know what is the beginning of a transaction?
        throw new DataAccessException("begin failed: was expecting autocommit to be true");
      }
      data.put("connection", connection);
      connection.setAutoCommit(false);
    } catch (SQLException e) {
      throw new DataAccessException("begin failed", e);
    }
  }

  @Override
  public final void commit(TransactionContext ctx) {
    Map<Object, Object> data = ctx.configuration().data();
    int counter = (int)data.compute("counter", (k, v) -> (v == null) ? 1 : (int) v - 1);
    if (counter > 0) {
      // We only commit the top-level transaction
      return;
    }
    try {
      Connection connection = (Connection)data.remove("connection");
      connection.commit();
      connection.setAutoCommit(true);
      provider.release(connection);
    } catch (SQLException e) {
      throw new DataAccessException("commit failed", e);
    }
  }

  @Override
  public final void rollback(TransactionContext ctx) {
    // Rollbacks can happen in any sub-transaction.
    try {
      Map<Object, Object> data = ctx.configuration().data();
      Connection connection = (Connection)data.remove("connection");
      connection.rollback();
      connection.setAutoCommit(true);
      // reset the counter. Do we care about concurrency?
      data.put("counter", 0);
      provider.release(connection);
    } catch (SQLException e) {
      throw new DataAccessException("rollback failed", e);
    }
  }
}
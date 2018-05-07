/**
 * 
 */
package transactions;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;

/**
 * @author Frederick
 * 
 *         consolidation exercise for transactions
 *
 */
public class Transactions {
	private static String jdbcUrl = "jdbc:postgresql://baasu.db.elephantsql.com:5432/zagfmozf";
	private static String username = "zagfmozf";
	private static String password = "Uj1zrCPaMm0MlmQaJZ5tRCjkcYoGXboU";

	/**
	 * method to manage the credit of DB customer & cash
	 * 
	 * @param conn
	 * @param productId
	 * @param nbCredit
	 */
	private static void credit(Connection conn, int productId, int nbCredit) {
		String sql = "UPDATE customer SET credit = credit + ? WHERE pk_id = ?";
		String sql2 = "UPDATE cash SET amount = amount + ? WHERE pk_id = 1";
		try (PreparedStatement ps = conn.prepareStatement(sql); PreparedStatement ps2 = conn.prepareStatement(sql2)) {
			conn.setAutoCommit(false);
			ps.setInt(1, nbCredit);
			ps.setInt(2, productId);
			int result = ps.executeUpdate();
			ps2.setInt(1, nbCredit);
			result += ps2.executeUpdate();
			if (result == 2) {
				conn.commit();
				conn.setAutoCommit(true);
			} else {
				System.err.println("Sorry, we have a technical problem to credit the customer.");
				conn.rollback();
			}
		} catch (SQLException e) {
			System.err.println("Sorry, we have a technical problem to access at the database.");
			e.printStackTrace();
			try {
				conn.rollback();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}

	/**
	 * method to add a product quantity in database
	 * 
	 * @param conn
	 * @param productId
	 * @param qty
	 */
	private static void addToInventory(Connection conn, int productId, int qty) {
		String sql = "UPDATE product SET qty = qty + ? WHERE pk_id = ?";
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setInt(1, qty);
			ps.setInt(2, productId);
			int result = ps.executeUpdate();
			if (result == 1) {
				System.out.println("The product quantity has been added.");
			} else {
				System.err.println("the product quantity has not been added.");
			}
		} catch (SQLException e) {
			System.err.println("Sorry, we have a technical problem to access at the database to add product quantity.");
			e.printStackTrace();
		}
	}

	/**
	 * method to register the customer purchase
	 * @param conn
	 * @param clientId
	 * @param productId
	 * @param qty
	 */
	private static void sell(Connection conn, int clientId, int productId, int qty) {
		String sql = "UPDATE customer SET credit = (credit - ((SELECT price FROM product WHERE pk_id=?)*?)) WHERE pk_id = ?";
		String sql2 = "UPDATE product SET qty = (qty - ?) WHERE pk_id = ?";
		String sql3 = "UPDATE sales_log SET qty = qty + ? WHERE fk_product_id = ?";
		try (PreparedStatement ps = conn.prepareStatement(sql);
				PreparedStatement ps2 = conn.prepareStatement(sql2);
				PreparedStatement ps3 = conn.prepareStatement(sql3)) {
			conn.setAutoCommit(false);
			ps.setInt(1, productId);
			ps.setInt(2, qty);
			ps.setInt(3, clientId);
			int result = ps.executeUpdate();
			ps2.setInt(1, qty);
			ps2.setInt(2, productId);
			result += ps2.executeUpdate();
			ps3.setInt(1, qty);
			ps3.setInt(2, productId);
			result += ps3.executeUpdate();
			if (result == 3) {
				conn.commit();
				conn.setAutoCommit(true);
				System.out.println("Your purchase has been registered.");
			} else {
				conn.rollback();
				System.err.println("Your purchase has not been registered.");
			}
		} catch (SQLException e) {
			System.err
					.println("Sorry, we have a technical problem to access at the database to register your purchase.");
			e.printStackTrace();
			try {
				conn.rollback();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}

	/**
	 * main body to generate automatically requests
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		int clientId = 1;
		int productId = 1;
		try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
			Random rng = new Random();
			while (true) {
				credit(conn, clientId, rng.nextInt(100) + 1);
				addToInventory(conn, productId, rng.nextInt(10) + 1);
				sell(conn, clientId, productId, rng.nextInt(10) + 1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}

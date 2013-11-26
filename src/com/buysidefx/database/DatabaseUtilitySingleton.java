package com.buysidefx.database;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Vector;

import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.log4j.Logger;

import com.buysidefx.exceptions.DatabaseException;
import com.buysidefx.props.PropertiesLoader;



public enum DatabaseUtilitySingleton {
	INSTANCE;
	private static final Logger logger = Logger
			.getLogger(DatabaseUtilitySingleton.class);

	private static GenericObjectPool<Connection> poolMgr = null;

	private static boolean debug = false;

	static {

		String startString = DatabaseUtilitySingleton.class.getCanonicalName()
				+ " static initializer";
		logger.info(startString);

		Properties props = PropertiesLoader.loadProperties("database", null);
		String serverName = props.getProperty("serverName");
		if (serverName == null)
			serverName = "localhost";
		String portStr = props.getProperty("port");
		if (portStr == null)
			portStr = "8082";
		int port = Integer.parseInt(portStr);
		String databaseName = props.getProperty("databaseName");
		if (databaseName == null)
			databaseName = "bfx";
		String user = props.getProperty("user");
		if (user == null)
			user = "marathon";
		String password = props.getProperty("password");
		if (password == null)
			password = "marathon";
		int maxConnections = 100;


		String debugStr = props.getProperty("debug");
		if (debugStr != null) {
			if (debugStr.toUpperCase(). equals("TRUE")) {
				debug = true;
			}
		}


		String url = "jdbc:postgresql://" + serverName + ":" + port + "/" + databaseName;


		try {
			logger.info("Creating ConnectionPoolDataSource with: " + serverName + " " + port + "  " + databaseName + " " + user + " and a password");
			poolMgr = new GenericObjectPool<Connection>(null);
			poolMgr.setMaxActive(maxConnections);
			ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(url, user, password);
			new PoolableConnectionFactory(connectionFactory, poolMgr, null, null, false, true);

		} catch (Exception ex) {
			logger.error(ex.getMessage());
		}


	}


	private String stackTraceToString(Throwable t) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		t.printStackTrace(pw);
		return sw.toString(); // stack trace as a string
	}

	private Connection getConnection() throws DatabaseException {
		Connection conn = null;
		if (poolMgr != null) {
			synchronized (this) {
				try {
					conn = poolMgr.borrowObject();
				} catch (NoSuchElementException e) {
					logger.error("NoSuchElement Exception in DatabaseUtilitySingleton.INSTANCE.getConnection(): "+stackTraceToString(e),e);
					DatabaseException de = new DatabaseException(e.getMessage(),e);
					throw de;
				} catch (IllegalStateException e) {
					logger.error("IllegalStateException Exception in DatabaseUtilitySingleton.INSTANCE.getConnection(): "+stackTraceToString(e),e);
					DatabaseException de = new DatabaseException(e.getMessage(),e);
					throw de;

				} catch (Exception e) {
					logger.error("General Exception in DatabaseUtilitySingleton.INSTANCE.getConnection(): "+stackTraceToString(e),e);
					DatabaseException de = new DatabaseException(e.getMessage(),e);
					throw de;
				}
			}
		} else {
			logger.error("PoolManager is null in DatabaseUtilitySingleton.INSTANCE.getConnection()");
			DatabaseException de = new DatabaseException("PoolManager is null",null);
			throw de;
		}
		return conn;
	}


	private PreparedStatement getPreparedStatement(Connection conn, String sql) throws DatabaseException {
		try {
			return conn.prepareStatement(sql); // create a statement

		} catch (SQLException e) {
			logger.error("SQLException in DatabaseUtilitySingleton.INSTANCE.getPreparedStatement(): "+stackTraceToString(e),e);
			SQLException sq = e.getNextException();
			while (sq != null) {
				logger.error("Nested SQLException in DatabaseUtilitySingleton.INSTANCE.getPreparedStatement(): "+sq.getMessage(), sq);
				sq = sq.getNextException();
			}

			DatabaseException de = new DatabaseException(e.getMessage(),e);
			throw de;
		}

	}


	private void freeResources(ResultSet rs, PreparedStatement pstmt, Connection conn) {
		if (rs != null) {
			try {
				rs.close();
			} catch (Exception ignore) {}
		}
		if (pstmt != null) {
			try {
				pstmt.close();
			} catch (Exception ignore) {}
		}
		if (conn != null) {
			try {
				conn.close();
			} catch (Exception ignore) {}
		}
	}


	public Boolean updateNettingAlgo(List<Long> tradeIds,String nettingAlgo)
			throws DatabaseException {

		String method = "updateNettingAlgo";
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Boolean success = null;


		conn = getConnection();
		pstmt = getPreparedStatement(conn, "select bfx.fn_update_order_netting_algo(?,?);");

		try {
			pstmt.setArray(1,conn.createArrayOf("integer",tradeIds.toArray(new Long[0])));
			pstmt.setString(2, nettingAlgo);

			if (debug) 
				logger.info("DatabaseUtilitySingleton "+method+" execution: " + pstmt);

			rs = pstmt.executeQuery(); // execute

			while (rs.next()) { // Position the cursor
				success = rs.getBoolean(1); // Retrieve the first column value
			}
		}  catch (SQLException e) {
			logger.error("SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+stackTraceToString(e),e);
			SQLException sq = e.getNextException();
			while (sq != null) {
				logger.error("Nested SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+sq.getMessage(), sq);
				sq = sq.getNextException();
			}

			DatabaseException de = new DatabaseException(e.getMessage(),e);
			throw de;


		} finally {

			freeResources(rs, pstmt, conn);
		}
		return success;
	}


	public Boolean upsertOrderStateAndEvent(List<Long> tradeIds,String targetOrderState,
			String eventShortCode,String eventSource,Long userID)
					throws DatabaseException {

		String method = "acceptNettingResults";
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Boolean success = null;

		conn = getConnection();
		pstmt = getPreparedStatement(conn, "select bfx.fn_upsert_order_state_and_event(?,?,?,?,?,?);");


		try {

			pstmt.setArray(1,conn.createArrayOf("integer",tradeIds.toArray(new Long[0])));
			pstmt.setString(2, null);
			pstmt.setString(3, eventShortCode);
			pstmt.setString(4, eventSource);
			pstmt.setInt(5, userID.intValue());
			pstmt.setString(6, "Accept Netting Results");

			if (debug) 
				logger.info("DatabaseUtilitySingleton "+method+" execution: " + pstmt);

			
			rs = pstmt.executeQuery(); // execute
			while (rs.next()) { // Position the cursor
				success = rs.getBoolean(1); // Retrieve the first column value
			}
			
		}  catch (SQLException e) {
			logger.error("SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+stackTraceToString(e),e);
			SQLException sq = e.getNextException();
			while (sq != null) {
				logger.error("Nested SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+sq.getMessage(), sq);
				sq = sq.getNextException();
			}

			DatabaseException de = new DatabaseException(e.getMessage(),e);
			throw de;


		} finally {

			freeResources(rs, pstmt, conn);
		}
		return success;
	}


	public Boolean lockOrders(List<Long> tradeIds, Integer userId)
			throws DatabaseException {
		String method = "lockOrders";
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Boolean success = null;

		conn = getConnection();
		pstmt = getPreparedStatement(conn, "select fn_lock_orders(?,?);");


		try {
			pstmt.setArray(1,conn.createArrayOf("integer",tradeIds.toArray(new Long[0])));
			pstmt.setInt(2, userId);


			if (debug) 
				logger.info("DatabaseUtilitySingleton "+method+" execution: " + pstmt);

			rs = pstmt.executeQuery(); // execute

			while (rs.next()) { // Position the cursor
				success = rs.getBoolean(1); // Retrieve the first column value
			}
		}  catch (SQLException e) {
			logger.error("SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+stackTraceToString(e),e);
			SQLException sq = e.getNextException();
			while (sq != null) {
				logger.error("Nested SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+sq.getMessage(), sq);
				sq = sq.getNextException();
			}

			DatabaseException de = new DatabaseException(e.getMessage(),e);
			throw de;


		} finally {

			freeResources(rs, pstmt, conn);
		}
		return success;
	}


	public Boolean unLockOrders(List<Long> tradeIds, Integer userId)
			throws DatabaseException {
		String method = "unLockOrders";
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Boolean success = null;

		conn = getConnection();
		pstmt = getPreparedStatement(conn, "select fn_unlock_orders(?,?);");


		try {

			pstmt.setArray(1,conn.createArrayOf("integer",tradeIds.toArray(new Long[0])));
			pstmt.setInt(2, userId);

			if (debug) 
				logger.info("DatabaseUtilitySingleton "+method+" execution: " + pstmt);

			rs = pstmt.executeQuery(); // execute

			while (rs.next()) { // Position the cursor
				success = rs.getBoolean(1); // Retrieve the first column value
			}
		}  catch (SQLException e) {
			logger.error("SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+stackTraceToString(e),e);
			SQLException sq = e.getNextException();
			while (sq != null) {
				logger.error("Nested SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+sq.getMessage(), sq);
				sq = sq.getNextException();
			}

			DatabaseException de = new DatabaseException(e.getMessage(),e);
			throw de;


		} finally {

			freeResources(rs, pstmt, conn);
		}
		return success;
	}


	public ArrayList<Order> getOrderDetailsArray(List<Long> orderIds)
			throws DatabaseException {

		String method = "getOrderDetailsArray";
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		ArrayList<Order> orders = new ArrayList<>();

		conn = getConnection();
		pstmt = getPreparedStatement(conn, "select * from fn_get_order_details_array(?);");


		try {

			pstmt.setArray(1,conn.createArrayOf("integer",orderIds.toArray(new Long[0])));
			if (debug) 
				logger.info("DatabaseUtilitySingleton "+method+" execution: " + pstmt);

			rs = pstmt.executeQuery(); // execute

			while (rs.next()) { // Position the cursor
				Order o = new Order();
				o.setOrderID(rs.getLong("order_id"));
				o.setOmsNumber(rs.getString("oms_number"));
				o.setOrderType(rs.getString("order_type"));
				o.setOrderSubType(rs.getString("order_sub_type"));
				o.setOrderSubStatus(rs.getString("order_sub_status"));
				o.setInputUserEmail(rs.getString("input_user_email"));                

				o.setOrderSource(rs.getString("order_source"));
				o.setTradeDate(new Date(rs.getDate("trade_date").getTime()));
				o.setCcyPair(rs.getString("ccy_pair"));
				o.setCcyBase(rs.getString("ccy_base"));
				o.setCcyBaseIsRestricted(rs.getBoolean("base_is_restricted"));
				o.setCcyTerm(rs.getString("ccy_term"));
				o.setCcyTermIsRestricted(rs.getBoolean("term_is_restricted"));
				o.setCcyDealt(rs.getString("ccy_dealt"));
				o.setDealtIsBase(rs.getBoolean("dealt_is_base"));
				o.setCcyContra(rs.getString("ccy_contra"));
				o.setCounterpartySelectionActionCode(rs.getString("counterparty_selection_action_code"));
				o.setExecutionType(rs.getString("execution_type"));

				o.setNumLegs(rs.getLong("num_legs"));
				o.setNearSide(rs.getString("near_side"));
				o.setNearBuySellBase(rs.getString("near_buy_sell_base"));
				o.setNearValueDate(new Date(rs.getDate("near_value_date").getTime()));
				o.setNearDealtQuantity(rs.getBigDecimal("near_dealt_quantity"));
				o.setNearDealtQuantityUSD(rs.getBigDecimal("near_dealt_quantity_usd"));
				o.setNearLegComponentOrderCount(rs.getLong("near_leg_account_count"));

				//Begin Near Leg...                
				Vector<NearLegAccount> nearOrderAccountsVector = new Vector<NearLegAccount>();
				o.setNearLegAccount(nearOrderAccountsVector);

				Array fieldPtr = rs.getArray("near_accounts_array");
				ResultSet rs2 = fieldPtr.getResultSet();

				while (rs2.next()) {
					String[] fields = parsePostgresArray(rs2.getString(2));
					NearLegAccount nearLegAccount = new NearLegAccount();
					nearOrderAccountsVector.add(nearLegAccount);
					nearLegAccount.setOrder(o);

					NearLegTradeAwayComponent nearTradeAway = new NearLegTradeAwayComponent();
					nearLegAccount.setNearLegTradeAwayComponent(nearTradeAway);
					nearTradeAway.setNearLegAccount(nearLegAccount);

					nearLegAccount.setAccountBaseCcy(fields[0]);
					nearLegAccount.setAccountIsNettable(Boolean.parseBoolean(fields[1]));
					nearLegAccount.setAccountNumber(fields[2]);
					nearLegAccount.setDealtQuantity(new BigDecimal(fields[3]));
					nearLegAccount.setHasCounterpartyCreditRestrictions(Boolean.parseBoolean(fields[4]));
					nearLegAccount.setHasOwnerCounterpartyRestrictions(Boolean.parseBoolean(fields[5]));
					nearLegAccount.setMinNettableAmountInUSD(new BigDecimal(fields[6]));
					nearLegAccount.setSide(fields[7]);

					nearTradeAway.setExpectedCustodianSlippage(new BigDecimal(fields[8]));
					nearTradeAway.setTradeAwayFeeUSD(new BigDecimal(fields[9]));
					nearTradeAway.setTradeAwayFeeUSDTermsCrossRate(new BigDecimal(fields[10]));
					nearTradeAway.setTradeAwayFeeTermsBaseCrossRate(new BigDecimal(fields[11]));    
				}
				//End Near Leg                

				//                0 la_account_base_currency,
				//                1 la_is_nettable,
				//                2 la_account_no,
				//                3 la_dealt_quantity,
				//                4 la_has_counterparty_restrictions,
				//                5 la_has_owner_counterparty_restrictions,
				//                6 la_min_nettable_value_in_usd,
				//                7 la_side,
				//                8 la_expected_custodian_slippage,
				//                9 la_tradeaway_fee_usd,
				//                10 la_tradeaway_fee_usd_terms_crossrate,
				//                11 la_tradeaway_fee_terms_base_crossrate

				if (o.getNumLegs() == 2) {

					o.setFarSide(rs.getString("far_side"));
					o.setFarBuySellBase(rs.getString("far_buy_sell_base"));                
					o.setFarValueDate(new Date(rs.getDate("far_value_date").getTime()));
					o.setFarDealtQuantity(rs.getBigDecimal("far_dealt_quantity"));
					o.setFarDealtQuantityUSD(rs.getBigDecimal("far_dealt_quantity_usd"));
					o.setFarLegComponentOrderCount(rs.getLong("far_leg_account_count"));

					//Begin Far Leg...                
					Vector<FarLegAccount> farOrderAccountsVector = new Vector<FarLegAccount>();
					o.setFarLegAccount(farOrderAccountsVector);


					Array fieldPtr2 = rs.getArray("far_accounts_array");
					ResultSet rs3 = fieldPtr2.getResultSet();

					while (rs3.next()) {
						String[] fields = parsePostgresArray(rs3.getString(2));
						FarLegAccount farLegAccount = new FarLegAccount();
						farOrderAccountsVector.add(farLegAccount);
						farLegAccount.setOrder(o);

						FarLegTradeAwayComponent farTradeAway = new FarLegTradeAwayComponent();
						farLegAccount.setFarLegTradeAwayComponent(farTradeAway);
						farTradeAway.setFarLegAccount(farLegAccount);

						farLegAccount.setAccountBaseCcy(fields[0]);
						farLegAccount.setAccountIsNettable(Boolean.parseBoolean(fields[1]));
						farLegAccount.setAccountNumber(fields[2]);
						farLegAccount.setDealtQuantity(new BigDecimal(fields[3]));
						farLegAccount.setHasCounterpartyCreditRestrictions(Boolean.parseBoolean(fields[4]));
						farLegAccount.setHasOwnerCounterpartyRestrictions(Boolean.parseBoolean(fields[5]));
						farLegAccount.setMinNettableAmountInUSD(new BigDecimal(fields[6]));
						farLegAccount.setSide(fields[7]);

						farTradeAway.setExpectedCustodianSlippage(new BigDecimal(fields[8]));
						farTradeAway.setTradeAwayFeeUSD(new BigDecimal(fields[9]));
						farTradeAway.setTradeAwayFeeUSDTermsCrossRate(new BigDecimal(fields[10]));
						farTradeAway.setTradeAwayFeeTermsBaseCrossRate(new BigDecimal(fields[11]));
					}
				}

				//End Far Leg                  

				o.setMaxRFQGroupsize(rs.getLong("max_rfq_groupsize"));

				o.setSplitActionCode(rs.getString("split_action_code"));
				//					o.setSplitMethod(rs.getString("split_method"));
				o.setSplitOverhangAction(rs.getString("split_overhang_action"));
				//					o.setSplitTargetOrderCount(rs.getLong("split_target_order_count"));
				o.setSplitTargetVolume(rs.getBigDecimal("split_target_volume"));

				//Counterpary Array                
				Vector<Counterparty> counterpartyVector = new Vector<Counterparty>();
				o.setCounterparty(counterpartyVector);

				Array counterpartyFields = rs.getArray("counterparty_candidates_array");
				ResultSet rs4 = counterpartyFields.getResultSet();

				while (rs4.next()) {

					String[] fields = parsePostgresArray(rs4.getString(2));
					Counterparty counterparty = new Counterparty();
					counterpartyVector.add(counterparty);
					counterparty.setOrder(o);                    	

					counterparty.setCptyAbbreviation(fields[1]);
					counterparty.setCptyVenue(fields[4]);
					counterparty.setDefaultCptyVolumeInOrder(new BigDecimal(fields[5]));
					counterparty.setIsAccountDefaultCpty(Boolean.parseBoolean(fields[6]));
					counterparty.setIsCreditPermitted(Boolean.parseBoolean(fields[7]));
					counterparty.setIsOwnerPermitted(Boolean.parseBoolean(fields[8]));
					counterparty.setRankingByValue(Long.parseLong(fields[9]));
					counterparty.setVenueShortCode(fields[3]);
					counterparty.setNumChildOrders(Long.parseLong(fields[10]));
				}
				//                    0 bank_id
				//                    1 bank_abbreviation
				//                    2 venue_id
				//                    3 venue_short_code
				//                    4 counterparty_venue
				//                    5 volume_in_order
				//                    6 is_account_default
				//                    7 is_credit_permitted
				//                    8 is_owner_permitted
				//                    9 ranking_by_value
				//                   10 numChildOrders

				orders.add(o);
			}

		}  catch (SQLException e) {
			logger.error("SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+stackTraceToString(e),e);
			SQLException sq = e.getNextException();
			while (sq != null) {
				logger.error("Nested SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+sq.getMessage(), sq);
				sq = sq.getNextException();
			}

			DatabaseException de = new DatabaseException(e.getMessage(),e);
			throw de;


		} finally {

			freeResources(rs, pstmt, conn);
		}
		return orders;
	}


	public Vector<Counterparty> getCounterpartyCandidates(Long orderID)
			throws DatabaseException {

		String method = "getCounterpartyCandidates";
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Vector<Counterparty> counterpartyVector = new Vector<Counterparty>();

		conn = getConnection();
		pstmt = getPreparedStatement(conn, "select * from fn_get_counterparty_candidates(?);");


		try {


			if (debug) 
				logger.info("DatabaseUtilitySingleton "+method+" execution: " + pstmt);

			rs = pstmt.executeQuery(); // execute

			while (rs.next()) { // Position the cursor
				Counterparty cpty = new Counterparty();
				counterpartyVector.add(cpty);

				cpty.setCptyAbbreviation(rs.getString("bank_abbreviation"));
				cpty.setCptyVenue(rs.getString("counterparty_venue"));
				cpty.setVenueShortCode(rs.getString("venue_short_code"));
				cpty.setDefaultCptyVolumeInOrder(rs.getBigDecimal("volume_in_order"));
				cpty.setIsAccountDefaultCpty(rs.getBoolean("is_account_default"));
				cpty.setIsCreditPermitted(rs.getBoolean("is_credit_permitted"));
				cpty.setIsOwnerPermitted(rs.getBoolean("is_owner_permitted"));
				cpty.setRankingByValue(rs.getLong("ranking_by_value"));

			}

		}  catch (SQLException e) {
			logger.error("SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+stackTraceToString(e),e);
			SQLException sq = e.getNextException();
			while (sq != null) {
				logger.error("Nested SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+sq.getMessage(), sq);
				sq = sq.getNextException();
			}

			DatabaseException de = new DatabaseException(e.getMessage(),e);
			throw de;


		} finally {

			freeResources(rs, pstmt, conn);
		}
		return counterpartyVector;
	}


	private String[] parsePostgresArray(String a) {

		String s = a.substring(a.indexOf('{')+1,a.indexOf('}'));
		//logger.info("s: "+s);

		String[] sa = s.split(",");

		return sa;
	}


	public ParentChildrenResults insertNetOrderAll(List<Long> orderIds, Integer userId, String timezone)
			throws DatabaseException {


		String method = "insertNetOrderAll";
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Integer[] orderIdsInt = new Integer[orderIds.size()];
		ParentChildrenResults parentChildrenResults = null;

		conn = getConnection();
		pstmt = getPreparedStatement(conn, "select * from fn_insert_net_order_all (?,?,?)");

		for (int i=0;i< orderIds.size();i++) {
			orderIdsInt[i] = orderIds.get(i).intValue();
		}

		try {
			//conn.createArrayOf("integer",tradeIds.toArray(new Long[0])
			pstmt.setArray(1,conn.createArrayOf("integer",orderIdsInt));
			pstmt.setInt(2, userId);
			pstmt.setString(3, timezone);

			if (debug) 
				logger.info("DatabaseUtilitySingleton "+method+" execution: " + pstmt);

			rs = pstmt.executeQuery(); // execute
			//advance cursor
			rs.next();
			parentChildrenResults = new ParentChildrenResults(rs,debug);
			if (debug) {
				logger.info("DatabaseUtilitySingleton "+method+" parentChildResult: " + parentChildrenResults);
			}


		}  catch (SQLException e) {
			logger.error("SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+stackTraceToString(e),e);
			SQLException sq = e.getNextException();
			while (sq != null) {
				logger.error("Nested SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+sq.getMessage(), sq);
				sq = sq.getNextException();
			}

			DatabaseException de = new DatabaseException(e.getMessage(),e);
			throw de;


		} finally {

			freeResources(rs, pstmt, conn);
		}
		return parentChildrenResults;
	}










	public List<ParentChildrenResults> acceptNetOrderBatch(Long batchId, List<Long> netGroupIds)
			throws DatabaseException {


		String method = "acceptNetOrderBatch";
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Integer[] netGroupIdsInt = new Integer[netGroupIds.size()];
		List<ParentChildrenResults> parentChildrenResultsList = new ArrayList<>();

		conn = getConnection();
		pstmt = getPreparedStatement(conn, "select * from fn_accept_net_order_batch (?,?)");


		for (int i=0;i< netGroupIds.size();i++) {
			netGroupIdsInt[i] = netGroupIds.get(i).intValue();
		}


		try {

			pstmt.setInt(1, batchId.intValue());
			pstmt.setArray(2,conn.createArrayOf("integer",netGroupIdsInt));


			if (debug) 
				logger.info("DatabaseUtilitySingleton "+method+" execution: " + pstmt);

			rs = pstmt.executeQuery(); // execute


			while (rs.next()) {
				parentChildrenResultsList.add( new ParentChildrenResults(rs,debug));	
			}


		}  catch (SQLException e) {
			logger.error("SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+stackTraceToString(e),e);
			SQLException sq = e.getNextException();
			while (sq != null) {
				logger.error("Nested SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+sq.getMessage(), sq);
				sq = sq.getNextException();
			}

			DatabaseException de = new DatabaseException(e.getMessage(),e);
			throw de;


		} finally {

			freeResources(rs, pstmt, conn);
		}
		return parentChildrenResultsList;


	}	





	public String getNettingCode(String executionType, String ccyDealt, String orderType, BigDecimal dealtQuantity)
			throws DatabaseException {
		String method = "getNettingCode";
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String nettingActionCode = null;

		conn = getConnection();
		pstmt = getPreparedStatement(conn, "select * from fn_rules_get_netting_code (?,?,?,?);");


		try {
			pstmt.setString(1,executionType);
			pstmt.setString(2, ccyDealt);
			pstmt.setString(3, orderType);
			pstmt.setBigDecimal(4, dealtQuantity);


			if (debug) 
				logger.info("DatabaseUtilitySingleton "+method+" execution: " + pstmt);

			rs = pstmt.executeQuery(); // execute

			while (rs.next()) { // Position the cursor
				nettingActionCode = rs.getString(1);
			}
		}  catch (SQLException e) {
			logger.error("SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+stackTraceToString(e),e);
			SQLException sq = e.getNextException();
			while (sq != null) {
				logger.error("Nested SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+sq.getMessage(), sq);
				sq = sq.getNextException();
			}

			DatabaseException de = new DatabaseException(e.getMessage(),e);
			throw de;


		} finally {

			freeResources(rs, pstmt, conn);
		}
		return nettingActionCode;
	}


	public Boolean clearNetOrders(List<Long> netOrderIds, Long userId, String timezone)
			throws DatabaseException {
		String method = "clearNetOrders";
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Boolean success = null;

		conn = getConnection();
		pstmt = getPreparedStatement(conn, "select fn_clear_net_orders (?,?,?);");


		try {

			pstmt.setArray(1,conn.createArrayOf("integer",netOrderIds.toArray(new Long[0])));
			pstmt.setInt(2, userId.intValue());
			pstmt.setString(3, timezone);


			if (debug) 
				logger.info("DatabaseUtilitySingleton "+method+" execution: " + pstmt);

			rs = pstmt.executeQuery(); // execute

			while (rs.next()) { // Position the cursor
				success = rs.getBoolean(1); // Retrieve the first column value
			}
		}  catch (SQLException e) {
			logger.error("SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+stackTraceToString(e),e);
			SQLException sq = e.getNextException();
			while (sq != null) {
				logger.error("Nested SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+sq.getMessage(), sq);
				sq = sq.getNextException();
			}

			DatabaseException de = new DatabaseException(e.getMessage(),e);
			throw de;


		} finally {

			freeResources(rs, pstmt, conn);
		}
		return success;
	}


	public Boolean insertOrderBanks(Long tradeId,String bankAbbreviation,String venueShortCode)
			throws DatabaseException {
		String method = "insertOrderBanks";
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Boolean success = null;

		conn = getConnection();
		pstmt = getPreparedStatement(conn, "select bfx.fn_insert_order_banks(?,?,?);");


		try {
			pstmt.setInt(1,tradeId.intValue());
			pstmt.setString(2,bankAbbreviation);
			pstmt.setString(3,venueShortCode);

			if (debug) 
				logger.info("DatabaseUtilitySingleton "+method+" execution: " + pstmt);

			rs = pstmt.executeQuery(); // execute

			while (rs.next()) { // Position the cursor
				success = rs.getBoolean(1); // Retrieve the first column value
			}
		}  catch (SQLException e) {
			logger.error("SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+stackTraceToString(e),e);
			SQLException sq = e.getNextException();
			while (sq != null) {
				logger.error("Nested SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+sq.getMessage(), sq);
				sq = sq.getNextException();
			}

			DatabaseException de = new DatabaseException(e.getMessage(),e);
			throw de;


		} finally {

			freeResources(rs, pstmt, conn);
		}
		return success;
	}


	public ArrayList<CashToTradeExposure> getCashToTradeExposures(List<Long> ids)
			throws DatabaseException {
		String method = "getCashToTradeExposures";
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		ArrayList<CashToTradeExposure> cashToTradeExposures = null;

		conn = getConnection();
		pstmt = getPreparedStatement(conn, "select * from bfx.fn_get_cash_exposures (?);");


		try {

			pstmt.setArray(1,conn.createArrayOf("integer",ids.toArray(new Long[0])));

			if (debug) 
				logger.info("DatabaseUtilitySingleton "+method+" execution: " + pstmt);

			rs = pstmt.executeQuery(); // execute

			cashToTradeExposures = CashToTradeExposureFactory.generateCashToTradeExposures(rs, debug);


		}  catch (SQLException e) {
			logger.error("SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+stackTraceToString(e),e);
			SQLException sq = e.getNextException();
			while (sq != null) {
				logger.error("Nested SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+sq.getMessage(), sq);
				sq = sq.getNextException();
			}

			DatabaseException de = new DatabaseException(e.getMessage(),e);
			throw de;


		} finally {

			freeResources(rs, pstmt, conn);
		}
		return cashToTradeExposures;
	}


	public Long insertCashToTradeStages(Long cashDetailsId, String acctNumber, String orderType,
			java.util.Date nearValueDate, String ccyPair, String baseCcy, String termsCcy, String dealtCcy, String executionType,
			String targetValueDateTenor)
					throws DatabaseException {
		String method = "insertCashToTradeStages";
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Long ret = null;

		conn = getConnection();
		pstmt = getPreparedStatement(conn, "select * from bfx.fn_insert_cash_to_trade_stages_from_corticon (?,?,?,?,?,?,?,?,?,?);");


		try {

			pstmt.setInt(1,cashDetailsId.intValue());
			pstmt.setString(2, acctNumber);
			pstmt.setString(3,orderType);
			pstmt.setDate(4,new java.sql.Date(nearValueDate.getTime()));
			pstmt.setString(5,ccyPair);
			pstmt.setString(6, baseCcy);
			pstmt.setString(7, termsCcy);
			pstmt.setString(8,dealtCcy);
			pstmt.setString(9,executionType);
			pstmt.setString(10,targetValueDateTenor);


			if (debug) 
				logger.info("DatabaseUtilitySingleton "+method+" execution: " + pstmt);

			rs = pstmt.executeQuery(); // execute

			while(rs.next()) {
				ret = rs.getLong(1); // Retrieve the first column value
			}



		}  catch (SQLException e) {
			logger.error("SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+stackTraceToString(e),e);
			SQLException sq = e.getNextException();
			while (sq != null) {
				logger.error("Nested SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+sq.getMessage(), sq);
				sq = sq.getNextException();
			}

			DatabaseException de = new DatabaseException(e.getMessage(),e);
			throw de;


		} finally {

			freeResources(rs, pstmt, conn);
		}
		return ret;
	}


	public Long insertOrderFromCashExposure(Long cashDetailsId)
			throws DatabaseException {
		String method = "insertOrderFromCashExposure";
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Long ret = null;

		conn = getConnection();
		pstmt = getPreparedStatement(conn, "select * from bfx.fn_insert_order_from_cash_exposure (?);");


		try {
			pstmt.setInt(1,cashDetailsId.intValue());

			if (debug) 
				logger.info("DatabaseUtilitySingleton "+method+" execution: " + pstmt);

			rs = pstmt.executeQuery(); // execute

			while(rs.next()) {
				ret = rs.getLong(1); // Retrieve the first column value
			}

		}  catch (SQLException e) {
			logger.error("SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+stackTraceToString(e),e);
			SQLException sq = e.getNextException();
			while (sq != null) {
				logger.error("Nested SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+sq.getMessage(), sq);
				sq = sq.getNextException();
			}

			DatabaseException de = new DatabaseException(e.getMessage(),e);
			throw de;


		} finally {

			freeResources(rs, pstmt, conn);
		}
		return ret;
	}


	public ExecutionTypeData getExecutionTypeData(String account, String ccyPair, String ccyBase, String ccyTerm, 
			String ccyDealt, BigDecimal quantity, String orderType)
					throws DatabaseException {

		String method = "getExecutionTypeData";
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String executionType = null;
		BigDecimal dealtQuantityUSD = null;
		BigDecimal expectedCustodianSlippage = null;
		BigDecimal tradeAwayFeeUSD = null;
		ExecutionTypeData etd = null;

		conn = getConnection();
		pstmt = getPreparedStatement(conn, "select * from bfx.fn_rules_get_execution_type(?,?,?,?);");

		try {

			try {
				pstmt.setString(1, account);
				pstmt.setString(2, ccyDealt);
				pstmt.setBigDecimal(3, quantity);
				pstmt.setString(4,orderType);		

				if (debug) 
					logger.info("DatabaseUtilitySingleton "+method+" execution: " + pstmt);

				rs = pstmt.executeQuery(); // execute

				while(rs.next()) {
					executionType = rs.getString("lut_execution_type"); // Retrieve the first column value
					dealtQuantityUSD = rs.getBigDecimal("lut_dealt_quantity_usd");

				}
			}  catch (SQLException e) {
				logger.error("SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+stackTraceToString(e),e);
				SQLException sq = e.getNextException();
				while (sq != null) {
					logger.error("Nested SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+sq.getMessage(), sq);
					sq = sq.getNextException();
				}

				DatabaseException de = new DatabaseException(e.getMessage(),e);
				throw de;


			}



			pstmt = getPreparedStatement(conn, "select * from bfx.fn_get_account_tradeaway_components(?,?,?,?,?);");
			try {

				pstmt.setString(1, account);
				pstmt.setString(2, ccyPair);
				pstmt.setString(3, ccyBase);
				pstmt.setString(4, ccyTerm);
				pstmt.setString(5, ccyDealt);

				if (debug) 
					logger.info("DatabaseUtilitySingleton "+method+" execution: " + pstmt);

				rs = pstmt.executeQuery(); // execute

				while (rs.next()) { // Position the cursor
					expectedCustodianSlippage = rs.getBigDecimal("expected_custodian_slippage"); // Retrieve the first column value
					tradeAwayFeeUSD = rs.getBigDecimal("tradeaway_fee_usd");

				}

			}  catch (SQLException e) {
				logger.error("SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+stackTraceToString(e),e);
				SQLException sq = e.getNextException();
				while (sq != null) {
					logger.error("Nested SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+sq.getMessage(), sq);
					sq = sq.getNextException();
				}

				DatabaseException de = new DatabaseException(e.getMessage(),e);
				throw de;
			}

			etd = new ExecutionTypeData();
			etd.setExecutionType(executionType);
			etd.setDealtQuantityUSD(dealtQuantityUSD);
			etd.setExpectedCustodianSlippage(expectedCustodianSlippage);
			etd.setTradeAwayFeeUSD(tradeAwayFeeUSD);


		} finally {

			freeResources(rs, pstmt, conn);
		}
		return etd;
	}


	public Long getSplitOrderBatchId()
			throws DatabaseException {
		String method = "getSplitOrderBatchId";
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Long ret = null;

		conn = getConnection();
		pstmt = getPreparedStatement(conn, "select * from nextval('split_order_batch_seq');");


		try {

			if (debug) 
				logger.info("DatabaseUtilitySingleton "+method+" execution: " + pstmt);

			rs = pstmt.executeQuery(); // execute

			while(rs.next()) {
				ret = rs.getLong(1); // Retrieve the first column value
			}

		}  catch (SQLException e) {
			logger.error("SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+stackTraceToString(e),e);
			SQLException sq = e.getNextException();
			while (sq != null) {
				logger.error("Nested SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+sq.getMessage(), sq);
				sq = sq.getNextException();
			}

			DatabaseException de = new DatabaseException(e.getMessage(),e);
			throw de;


		} finally {

			freeResources(rs, pstmt, conn);
		}
		return ret;
	}


	public Long insertSplitOrderStages(Long batchId, Long parentOrderId,Long orderId,
			String omsNumber, String orderType, Date tradeDate, String account, String ccyPair,
			String ccyDealt, String nearSide, String farSide, BigDecimal nearQuantity, Date nearValueDate,
			BigDecimal farQuantity, Date farValueDate, Long userId, String userEmail, String orderSource,
			String executionType
			)
					throws DatabaseException {
		String method = "insertSplitOrderStages";
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Long ret = null;

		conn = getConnection();
		pstmt = getPreparedStatement(conn, "select * from bfx.fn_insert_split_order_stages (" +
				"?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);");

		try {
			pstmt.setInt(1,batchId.intValue());
			pstmt.setInt(2,parentOrderId.intValue());
			if (orderId == null)
				pstmt.setInt(3,0);
			else
				pstmt.setInt(3,orderId.intValue());
			pstmt.setString(4, omsNumber);
			pstmt.setString(5, orderType);
			pstmt.setDate(6,new java.sql.Date(tradeDate.getTime()));
			pstmt.setString(7, account);
			pstmt.setString(8, ccyPair);
			pstmt.setString(9, ccyDealt);
			pstmt.setString(10,nearSide);
			pstmt.setString(11, farSide);
			pstmt.setBigDecimal(12, nearQuantity);
			pstmt.setDate(13,new java.sql.Date(nearValueDate.getTime()));
			pstmt.setBigDecimal(14, farQuantity);

			if (farValueDate == null)
				pstmt.setDate(15, null);
			else
				pstmt.setDate(15, new java.sql.Date(farValueDate.getTime()));
			pstmt.setInt(16, userId.intValue());
			pstmt.setString(17, userEmail);
			pstmt.setString(18, orderSource);
			pstmt.setString(19, executionType);			


			if (debug) 
				logger.info("DatabaseUtilitySingleton "+method+" execution: " + pstmt);

			rs = pstmt.executeQuery(); // execute

			while(rs.next()) {
				ret = rs.getLong(1); // Retrieve the first column value
			}

		}  catch (SQLException e) {
			logger.error("SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+stackTraceToString(e),e);
			SQLException sq = e.getNextException();
			while (sq != null) {
				logger.error("Nested SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+sq.getMessage(), sq);
				sq = sq.getNextException();
			}

			DatabaseException de = new DatabaseException(e.getMessage(),e);
			throw de;


		} finally {

			freeResources(rs, pstmt, conn);
		}
		return ret;
	}


	public ArrayList<SplitOrderStagesDetail> getSplitOrderStagesDetail(Long batchId)
			throws DatabaseException {
		String method = "getSplitOrderStagesDetail";
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		ArrayList<SplitOrderStagesDetail> splitOrderStagesDetails = null;

		conn = getConnection();
		pstmt = getPreparedStatement(conn, "select * from bfx.fn_get_split_order_stages_detail (?);");


		try {
			pstmt.setInt(1,batchId.intValue());

			if (debug) 
				logger.info("DatabaseUtilitySingleton "+method+" execution: " + pstmt);

			rs = pstmt.executeQuery(); // execute




			splitOrderStagesDetails = SplitOrderStagesDetailFactory.generateSplitOrderStagesDetails(rs);

		}  catch (SQLException e) {
			logger.error("SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+stackTraceToString(e),e);
			SQLException sq = e.getNextException();
			while (sq != null) {
				logger.error("Nested SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+sq.getMessage(), sq);
				sq = sq.getNextException();
			}

			DatabaseException de = new DatabaseException(e.getMessage(),e);
			throw de;


		} finally {

			freeResources(rs, pstmt, conn);
		}
		return splitOrderStagesDetails;
	}


	public Boolean updateSplitOrderStagesExecutionType (Long splitOrderStagesId, String executionType)
			throws DatabaseException {
		String method = "updateSplitOrderStagesExecutionType";
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Boolean ret = null;

		conn = getConnection();
		pstmt = getPreparedStatement(conn, "select * from bfx.fn_update_split_order_stages_execution_type (?,?);");


		try {

			pstmt.setInt(1,splitOrderStagesId.intValue());
			pstmt.setString(2,executionType);


			if (debug) 
				logger.info("DatabaseUtilitySingleton "+method+" execution: " + pstmt);

			rs = pstmt.executeQuery(); // execute

			while (rs.next()) {
				ret = rs.getBoolean(1);
			}


		}  catch (SQLException e) {
			logger.error("SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+stackTraceToString(e),e);
			SQLException sq = e.getNextException();
			while (sq != null) {
				logger.error("Nested SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+sq.getMessage(), sq);
				sq = sq.getNextException();
			}

			DatabaseException de = new DatabaseException(e.getMessage(),e);
			throw de;


		} finally {

			freeResources(rs, pstmt, conn);
		}
		return ret;
	}


	public Boolean acceptSplitOrderBatch (Long batchId)
			throws DatabaseException {
		String method = "acceptSplitOrderBatch";
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Boolean ret = null;

		conn = getConnection();
		pstmt = getPreparedStatement(conn, "select * from fn_accept_split_order_batch (?);");


		try {

			pstmt.setInt(1,batchId.intValue());

			if (debug) 
				logger.info("DatabaseUtilitySingleton "+method+" execution: " + pstmt);

			rs = pstmt.executeQuery(); // execute

			while (rs.next()) {
				ret = rs.getBoolean(1);
			}

		}  catch (SQLException e) {
			logger.error("SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+stackTraceToString(e),e);
			SQLException sq = e.getNextException();
			while (sq != null) {
				logger.error("Nested SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+sq.getMessage(), sq);
				sq = sq.getNextException();
			}

			DatabaseException de = new DatabaseException(e.getMessage(),e);
			throw de;


		} finally {

			freeResources(rs, pstmt, conn);
		}
		return ret;
	}


	public Boolean clearSplitOrders (Long userId, Long parentOrderId, String timeZone)
			throws DatabaseException {
		String method = "clearSplitOrders";
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Boolean ret = null;

		conn = getConnection();
		pstmt = getPreparedStatement(conn, "select * from fn_clear_split_orders (?);");


		try {
			pstmt.setInt(1,parentOrderId.intValue());
			pstmt.setInt(2,userId.intValue());
			pstmt.setString(3,timeZone);

			if (debug) 
				logger.info("DatabaseUtilitySingleton "+method+" execution: " + pstmt);

			rs = pstmt.executeQuery(); // execute

			while (rs.next()) {
				ret = rs.getBoolean(1);
			}

		}  catch (SQLException e) {
			logger.error("SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+stackTraceToString(e),e);
			SQLException sq = e.getNextException();
			while (sq != null) {
				logger.error("Nested SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+sq.getMessage(), sq);
				sq = sq.getNextException();
			}

			DatabaseException de = new DatabaseException(e.getMessage(),e);
			throw de;


		} finally {

			freeResources(rs, pstmt, conn);
		}
		return ret;
	}






	//	CREATE OR REPLACE FUNCTION bfx.fn_update_order_execution_type (
	//			  p_order_ids integer [],
	//			  p_execution_type varchar
	//			)
	//			RETURNS boolean



	public Boolean updateOrderExecutionType (List<Long> orderIds, String executionType)
			throws DatabaseException {
		String method = "updateOrderExecutionType";
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Boolean ret = null;

		conn = getConnection();
		pstmt = getPreparedStatement(conn, "select * from bfx.fn_update_order_execution_type (?,?)");


		try {
			pstmt.setArray(1,conn.createArrayOf("integer",orderIds.toArray(new Long[0])));
			pstmt.setString(2,executionType);

			if (debug) 
				logger.info("DatabaseUtilitySingleton "+method+" execution: " + pstmt);

			rs = pstmt.executeQuery(); // execute

			while (rs.next()) {
				ret = rs.getBoolean(1);
			}

		}  catch (SQLException e) {
			logger.error("SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+stackTraceToString(e),e);
			SQLException sq = e.getNextException();
			while (sq != null) {
				logger.error("Nested SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+sq.getMessage(), sq);
				sq = sq.getNextException();
			}

			DatabaseException de = new DatabaseException(e.getMessage(),e);
			throw de;


		} finally {

			freeResources(rs, pstmt, conn);
		}
		return ret;
	}


	//	bfx.fn_update_order_counterparty_selection_action_code (
	//			  p_order_id integer,
	//			  p_counterparty_selection_action_code varchar
	//			)
	//


	public boolean updateOrderCounterpartySelectionActionCode(Long orderId,
			String counterpartySelectionActionCode) 
					throws DatabaseException {

		String method = "updateOrderCounterpartySelectionActionCode";
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Boolean ret = null;

		conn = getConnection();
		pstmt = getPreparedStatement(conn, "select * from bfx.fn_update_order_counterparty_selection_action_code (?,?)");


		try {
			pstmt.setInt(1,orderId.intValue());
			pstmt.setString(2,counterpartySelectionActionCode);

			if (debug) 
				logger.info("DatabaseUtilitySingleton "+method+" execution: " + pstmt);

			rs = pstmt.executeQuery(); // execute

			while (rs.next()) {
				ret = rs.getBoolean(1);
			}

		}  catch (SQLException e) {
			logger.error("SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+stackTraceToString(e),e);
			SQLException sq = e.getNextException();
			while (sq != null) {
				logger.error("Nested SQLException in DatabaseUtilitySingleton.INSTANCE.executeStatement(): "+sq.getMessage(), sq);
				sq = sq.getNextException();
			}

			DatabaseException de = new DatabaseException(e.getMessage(),e);
			throw de;


		} finally {

			freeResources(rs, pstmt, conn);
		}
		return ret;

	}




}
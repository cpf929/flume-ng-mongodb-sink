package org.apache.flume.sink.mongo;

import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.sink.mongo.constant.FieldName;
import org.apache.flume.sink.mongo.parse.HandlerFactory;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.WriteConcern;

public class MongoSink extends AbstractSink implements Configurable {
	private static Logger logger = LoggerFactory.getLogger(MongoSink.class);

	private static DateTimeParser[] parsers = { DateTimeFormat.forPattern("yyyy-MM-dd").getParser(),
			DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").getParser(),
			DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").getParser(),
			DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss Z").getParser(),
			DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS Z").getParser(),
			DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ").getParser(),
			DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ").getParser(),
			DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssz").getParser(),
			DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSz").getParser(), };
	public static DateTimeFormatter dateTimeFormatter = new DateTimeFormatterBuilder().append(null, parsers)
			.toFormatter();

	public static final String HOST = "host";
	public static final String PORT = "port";
	public static final String AUTHENTICATION_ENABLED = "authenticationEnabled";
	public static final String USERNAME = "username";
	public static final String PASSWORD = "password";
	public static final String MODEL = "model";
	public static final String DB_NAME = "db";
	public static final String COLLECTION = "collection";
	public static final String NAME_PREFIX = "MongSink_";
	public static final String BATCH_SIZE = "batch";
	public static final String AUTO_WRAP = "autoWrap";
	public static final String WRAP_FIELD = "wrapField";
	public static final String TIMESTAMP_FIELD = "timestampField";
	public static final String OPERATION = "op";
	public static final String PK = "_id";
	public static final String OP_INC = "$inc";
	public static final String OP_SET = "$set";
	public static final String OP_SET_ON_INSERT = "$setOnInsert";
	public static final String OP_ADD_TO_SET = "$addToSet";

	public static final boolean DEFAULT_AUTHENTICATION_ENABLED = false;
	public static final String DEFAULT_HOST = "localhost";
	public static final int DEFAULT_PORT = 27017;
	public static final String DEFAULT_DB = "events";
	public static final String DEFAULT_COLLECTION = "events";
	public static final int DEFAULT_BATCH = 100;
	private static final Boolean DEFAULT_AUTO_WRAP = false;
	public static final String DEFAULT_WRAP_FIELD = "log";
	public static final String DEFAULT_TIMESTAMP_FIELD = null;
	public static final char NAMESPACE_SEPARATOR = '.';
	public static final String OP_UPSERT = "upsert";
	public static final String EXTRA_FIELDS_PREFIX = "extraFields.";

	private static AtomicInteger counter = new AtomicInteger();

	private Mongo mongo;
	private DB db;

	private String host;
	private int port;
	private boolean authentication_enabled;
	private String username;
	private String password;
	private CollectionModel model;
	private String dbName;
	private String collectionName;
	private int batchSize;
	private boolean autoWrap;
	private String wrapField;
	private String timestampField;
	private final Map<String, String> extraInfos = new ConcurrentHashMap<String, String>();

	public void configure(Context context) {
		setName(NAME_PREFIX + counter.getAndIncrement());

		host = context.getString(HOST, DEFAULT_HOST);
		port = context.getInteger(PORT, DEFAULT_PORT);
		authentication_enabled = context.getBoolean(AUTHENTICATION_ENABLED, DEFAULT_AUTHENTICATION_ENABLED);
		if (authentication_enabled) {
			username = context.getString(USERNAME);
			password = context.getString(PASSWORD);
		} else {
			username = "";
			password = "";
		}
		model = CollectionModel.valueOf(context.getString(MODEL, CollectionModel.SINGLE.name()));
		dbName = context.getString(DB_NAME, DEFAULT_DB);
		collectionName = context.getString(COLLECTION, DEFAULT_COLLECTION);
		batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH);
		autoWrap = context.getBoolean(AUTO_WRAP, DEFAULT_AUTO_WRAP);
		wrapField = context.getString(WRAP_FIELD, DEFAULT_WRAP_FIELD);
		timestampField = context.getString(TIMESTAMP_FIELD, DEFAULT_TIMESTAMP_FIELD);
		extraInfos.putAll(context.getSubProperties(EXTRA_FIELDS_PREFIX));
		logger.info(
				"MongoSink {} context { host:{}, port:{}, authentication_enabled:{}, username:{}, password:{}, model:{}, dbName:{}, collectionName:{}, batch: {}, autoWrap: {}, wrapField: {}, timestampField: {} }",
				new Object[] { getName(), host, port, authentication_enabled, username, password, model, dbName,
						collectionName, batchSize, autoWrap, wrapField, timestampField });
	}

	@Override
	public synchronized void start() {
		logger.info("Starting {}...", getName());
		try {

			mongo = new Mongo(host, port);
			db = mongo.getDB(dbName);
		} catch (UnknownHostException e) {
			logger.error("Can't connect to mongoDB", e);
			return;
		}
		if (authentication_enabled) {
			boolean result = db.authenticate(username, password.toCharArray());
			if (result) {
				logger.info("Authentication attempt successful.");
			} else {
				logger.error(
						"CRITICAL FAILURE: Unable to authenticate. Check username and Password, or use another unauthenticated DB. Not starting MongoDB sink.\n");
				return;
			}
		}
		super.start();
		logger.info("Started {}.", getName());
	}

	public Status process() throws EventDeliveryException {
		logger.debug("{} start to process event", getName());

		Status status = Status.READY;
		try {
			status = parseEvents();
		} catch (Exception e) {
			logger.error("can't process events", e);
		}
		logger.debug("{} processed event", getName());
		return status;
	}

	private Status parseEvents() throws EventDeliveryException {
		Status status = Status.READY;
		Channel channel = getChannel();
		Transaction tx = null;
		Map<String, List<DBObject>> upsertMap = new HashMap<String, List<DBObject>>();
		try {
			tx = channel.getTransaction();
			tx.begin();

			for (int i = 0; i < batchSize; i++) {
				Event event = channel.take();
				if (event == null) {
					status = Status.BACKOFF;
					tx.rollback();
					return status;
				} else {
					processEvent(upsertMap, event);
				}
			}
			doUpsert(upsertMap);

			tx.commit();
		} catch (Exception e) {
			logger.error("can't process events, drop it!", e);
			if (tx != null) {
				tx.rollback();// commit to drop bad event, otherwise it will
								// enter
								// dead loop.
			}

		} finally {
			if (tx != null) {
				tx.close();
			}
		}
		return status;
	}

	private void doUpsert(Map<String, List<DBObject>> eventMap) {
		if (eventMap.isEmpty()) {
			logger.debug("eventMap is empty");
			return;
		}

		for (Map.Entry<String, List<DBObject>> entry : eventMap.entrySet()) {
			List<DBObject> docs = entry.getValue();
			logger.debug("collection: {}, length: {}", entry.getKey(), docs.size());

			int separatorIndex = entry.getKey().indexOf(NAMESPACE_SEPARATOR);
			String eventDb = entry.getKey().substring(0, separatorIndex);
			String collectionName = entry.getKey().substring(separatorIndex + 1);

			// Warning: please change the WriteConcern level if you need high
			// datum consistence.
			DB dbRef = mongo.getDB(eventDb);
			if (authentication_enabled) {
				boolean authResult = dbRef.authenticate(username, password.toCharArray());
				if (!authResult) {
					logger.error("Failed to authenticate user: " + username + " with password: " + password
							+ ". Unable to write events.");
					return;
				}
			}
			DBCollection collection = dbRef.getCollection(collectionName);
			for (DBObject doc : docs) {
				logger.debug("===doc:{}", doc);
				// 以设备id更新
				DBObject object = (DBObject) doc.get(OP_ADD_TO_SET);
				DBObject query = BasicDBObjectBuilder.start()
						.add(FieldName.field_deviceId, object.get(FieldName.field_deviceId)).get();

				CommandResult result = collection.update(query, doc, true, false, WriteConcern.ACKNOWLEDGED).getLastError();
				if (result.ok()) {
					String errorMessage = result.getErrorMessage();
					if (errorMessage != null) {
						logger.error("can't upsert documents with error: {} ", errorMessage);
						logger.error("with exception", result.getException());
					}
				} else {
					logger.error("can't get last error");
				}
			}
		}
	}

	private void processEvent(Map<String, List<DBObject>> eventMap, Event event) {
		switch (model) {
		case SINGLE:
			putSingleEvent(eventMap, event);

			break;
		case DYNAMIC:
			putDynamicEvent(eventMap, event);

			break;
		default:
			logger.error("can't support model: {}, please check configuration.", model);
		}
	}

	private void putDynamicEvent(Map<String, List<DBObject>> eventMap, Event event) {
		String eventCollection;
		Map<String, String> headers = event.getHeaders();
		String eventDb = headers.get(DB_NAME);
		eventCollection = headers.get(COLLECTION);

		if (!StringUtils.isEmpty(eventDb)) {
			eventCollection = eventDb + NAMESPACE_SEPARATOR + eventCollection;
		} else {
			eventCollection = dbName + NAMESPACE_SEPARATOR + eventCollection;
		}

		if (!eventMap.containsKey(eventCollection)) {
			eventMap.put(eventCollection, new ArrayList<DBObject>());
		}

		List<DBObject> documents = eventMap.get(eventCollection);
		addEventToList(documents, event);
	}

	private void putSingleEvent(Map<String, List<DBObject>> eventMap, Event event) {
		String eventCollection;
		eventCollection = dbName + NAMESPACE_SEPARATOR + collectionName;
		if (!eventMap.containsKey(eventCollection)) {
			eventMap.put(eventCollection, new ArrayList<DBObject>());
		}

		List<DBObject> docs = eventMap.get(eventCollection);
		addEventToList(docs, event);
	}

	private List<DBObject> addEventToList(List<DBObject> documents, Event event) {
		if (documents == null) {
			documents = new ArrayList<DBObject>(batchSize);
		}

		// DBObject eventJson;
		// byte[] body = event.getBody();
		// if (autoWrap) {
		// eventJson = new BasicDBObject(wrapField, new String(body));
		// } else {
		// try {
		// eventJson = (DBObject) JSON.parse(new String(body));
		// // eventJson.put(OP_SET, "$set");
		// } catch (Exception e) {
		// logger.error("Can't parse events: " + new String(body), e);
		// return documents;
		// }
		// }

		DBObject eventJson;
		try {
			eventJson = HandlerFactory.build(event);
		} catch (UnsupportedEncodingException e1) {
			logger.error(e1.getMessage(), e1);
			return documents;
		}

		// for (Map.Entry<String, String> entry : extraInfos.entrySet()) {
		// eventJson.put(entry.getKey(), entry.getValue());
		// }
		
		if(eventJson == null){
			return documents;
		}
		
		documents.add(eventJson);

		return documents;
	}

	public enum CollectionModel {
		DYNAMIC, SINGLE
	}
}

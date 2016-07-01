
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flume.sink.mongo.MongoSink;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;

public class MongoDriverTest {

	private MongoClient client;
	DB db;

	@Before
	public void init() throws UnknownHostException {
		// String url = String.format("mongodb://%s:%s@%s:%d/%s", "user",
		// "password", "localhost", 27017, "mydb");
		// MongoClientURI uri = new MongoClientURI(url);
		// client = new MongoClient(uri);
		client = new MongoClient("192.168.0.113", 27017);
		db = client.getDB("local");
	}

	@Test
	public void testFindOne() {
		DBCollection coll = db.getCollection("log");
		DBObject dbObject = coll.findOne();
		System.out.println(dbObject.toString());
	}

	@Test
	public void testInsert() throws JsonGenerationException, JsonMappingException, IOException {
		ObjectMapper mapper = new ObjectMapper();
		DBCollection coll = db.getCollection("log");

		String json = "{\"version\": 3,\"bid\": \"8062317240ab7409c0000576baab22939000b8515\",\"ip\": \"115.207.53.83\",\"user_agent\": \"Apache-HttpClient/4.3.4 (java 1.5)\",\"category\": 0,\"adx_type\": 0,\"adzinfo\": [{\"id\": 0,\"pid\": \"mm_26632357_12466943_47244027\",\"size\": \"320x150\",\"ad_bid_count\": 1,\"view_type\": [108],\"min_cpm_price\": 0,\"adz_location\": \"NA\",\"view_screen\": \"SCREEN_NA\",\"allowed_creative_level\": 1,\"publisher_filter_id\": [\"869f0e4fcf7bfba07eadae725358972f\"]}],\"timezone_offset\": 480,\"category_version\": 1,\"tid_version\": 1,\"excluded_ad_category\": [62303,62308,62314,62315,62317,70401,70499],\"mobile\": {\"ad_num\": 1,\"device\": {\"platform\": \"android\",\"os\": \"android\",\"os_version\": \"6.0.1\",\"network\": 1,\"operator\": 0,\"device_id\": \"AQ9w3Btd1QJOupGULDk+5TH6ElXE\",\"imei\": \"AQ9w3Btd1QJOupGULDk+5TH6ElXE\",\"mac\": \"ARF/3hdd1Ag8yZORLjY64z7re+9dRgM=\"},\"native_template_id\": [\"25\"],\"landing_type\": [2],\"native_ad_template\": [{\"native_template_id\": \"25\",\"areas\": [{\"id\": 0,\"creative_count\": 1,\"creative\": {\"required_fields\": [2,7,3,1],\"image_size\": \"640x320\"}}]}]},\"is_predicted_to_be_ignored\": true,\"ysbid\": \"8062317240ab7409c0000576baab22939000b8515\",\"bsdAdxType\": \"tanx\",\"infoType\": \"11\",\"package_name\": \"\",\"city\": \"衢州\",\"bidRequestTime\": \"1466673842179\"}";

		JSONObject object = com.alibaba.fastjson.JSON.parseObject(json);

		System.out.println(object.getJSONObject("mobile").getString("app_name"));

		@SuppressWarnings("rawtypes")
		Map map = mapper.readValue(json, Map.class);

		System.out.println(map);

		JsonObj obj = new JsonObj();
		obj.setId(1);
		obj.setName("cpf");
		System.out.println(mapper.writeValueAsString(obj));

		DBObject dbObject = (DBObject) JSON.parse(json);
		System.out.println(dbObject);
		System.out.println(dbObject.get("ysbid"));
		coll.insert(dbObject);

	}

	@Test
	public void testAddToSet() {

		// $addToSet，如果有， 就不追加
		DBCollection coll = db.getCollection("log");
		DBObject query = BasicDBObjectBuilder.start().add("imei", "aaasaadad1113fwwf").get();

		List<JsonObj> list = new ArrayList<JsonObj>();
		for (int i = 0; i < 2; i++) {
			list.add(new JsonObj(i, String.valueOf(i)));
		}

		BasicDBObject basicDBObject = new BasicDBObject("$addToSet",
				new BasicDBObject("geo", JSON.parse(com.alibaba.fastjson.JSON.toJSONString(list))));

		WriteResult result = coll.update(query, basicDBObject, true, true);

		System.out.println(result.getN());

	}

	@Test
	public void testPush() {

		// $push，不管有没有， 每次操作，都会追加
		DBCollection coll = db.getCollection("log");
		DBObject query = BasicDBObjectBuilder.start().add("imei", "aaasaadad1113fwwf").get();

		List<JsonObj> list = new ArrayList<JsonObj>();
		for (int i = 0; i < 2; i++) {
			list.add(new JsonObj(i, String.valueOf(i)));
		}

		BasicDBObject basicDBObject = new BasicDBObject("$pushAll",
				new BasicDBObject("geo", JSON.parse(com.alibaba.fastjson.JSON.toJSONString(list))));

		WriteResult result = coll.update(query, basicDBObject, true, true);

		System.out.println(result.getN());

	}

	@Test
	public void update() {
		DBObject query = BasicDBObjectBuilder.start().add("imei", "aaasaadad1113fwwf")
				.add("geo", new BasicDBObject("$elemMatch", new BasicDBObject("id", "11"))).get();
		DBCollection coll = db.getCollection("log");

		// multi update only works with $ operators
		BasicDBObject basicDBObject = new BasicDBObject("$set",
				new BasicDBObject("geo.$.time", System.currentTimeMillis() + ""));

		WriteResult result = coll.update(query, basicDBObject, true, true);
		System.out.println(result.getN());
	}

	/**
	 * 更新时同时执行多种操作， 有些字段每次都更新， 有些字段只插入， 有些字段要追加
	 */
	@Test
	public void testObjectBuilder() {

		DBObject query = BasicDBObjectBuilder.start().add("imei", "aaasaadad1113fwwf").get();
		DBCollection coll = db.getCollection("log");

		// multi update only works with $ operators
		BasicDBObjectBuilder doc_builder = BasicDBObjectBuilder.start();

		// coll.drop();
		//
		// doc_builder.add(MongoSink.OP_SET, new BasicDBObject("name",
		// "cpf").append("age", "23").append("app_name", "腾讯新闻"));
		//
		// doc_builder.add(MongoSink.OP_SET_ON_INSERT, new
		// BasicDBObject().append("type", "2221"));
		//
		// doc_builder.add("$addToSet",
		// new BasicDBObject("geo",
		// JSON.parse(com.alibaba.fastjson.JSON.toJSONString(new JsonObj(32,
		// "333"))))
		// .append("ip", "192.168.1.1"));
		// // doc_builder.add("imei", "aaasaadad1113fwwf");

		BasicDBObject appInfo = new BasicDBObject();

		BasicDBObject appObj = new BasicDBObject("appName", "腾讯新闻").append("package", "com.tencent.news").append("time",
				"11111111111");

		appInfo.append("appList.com_netstat_news", appObj);

		doc_builder.add("$set", appInfo);

		DBObject doc = doc_builder.get();

		System.out.println(doc);

		WriteResult result = coll.update(query, doc, true, true);
		System.out.println(result.getN());
	}

	@Test
	public void findByCon() {
		DBObject query = BasicDBObjectBuilder.start().add("imei", "aaasaadad1113fwwf").get();
		DBCollection coll = db.getCollection("log");
		DBCursor cursor = coll.find(query);

		while (cursor.hasNext()) {
			System.out.println(cursor.next().toString());
		}
	}

}

class JsonObj {

	private Integer id;
	private String name;

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public JsonObj() {
	}

	public JsonObj(Integer id, String name) {
		super();
		this.id = id;
		this.name = name;
	}

}

import java.util.List;
import java.util.Properties;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ReflectionStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.snowplowanalytics.hive.serde.CfLogDeserializer;

public class CfLogDeserializerTest {
	@Test
	public void runTest() {
		CfLogDeserializer serDe;
		try {
			serDe = new CfLogDeserializer();
		} catch (SerDeException e1) {
			e1.printStackTrace();
			Assert.fail("CfLogDeserializer constructor raised an exception");
			return;
		}
		Configuration conf = new Configuration();
		Properties tbl = new Properties();
		Text sample = new Text(
				"2012-03-16	11:45:01	ARN1	3422	195.78.71.32	GET	detlpfvsg0d9v.cloudfront.net	/ice.png	200	http://delivery.ads-creativesyndicator.com/adserver/www/delivery/afr.php?zoneid=103&cb=INSERT_RANDOM_NUMBER_HERE&ct0=INSERT_CLICKURL_HERE	Mozilla/5.0%20(Windows%20NT%206.0)%20AppleWebKit/535.11%20(KHTML,%20like%20Gecko)%20Chrome/17.0.963.79%20Safari/535.11	&ad_ba=1884&ad_ca=547&ad_us=a1088f76c6931b0a26228dc3bde321d7&r=481413&urlref=http%253A%252F%252Fwww.fantasyfootballscout.co.uk%252F&_id=b41cf6859dccd8ce&_ref=http%253A%252F%252Fwww.fantasyfootballscout.co.uk%252F&pdf=1&qt=0&realp=0&wma=0&dir=1&fla=1&java=1&gears=0&ag=1&res=1920x1200&cookie=1");
		// Text sample = new
		// Text("02/01/2011 01:13:12 LAX1 2390282 192.0.2.202 GET www.singalong.com /soundtrack/happy.mp3 304 www.unknownsingers.com Mozilla/4.0%20(compatible;%20MSIE%207.0;%20Windows%20NT%205.1) a=b&c=d");
		try {
			serDe.initialize(conf, tbl);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(System.err);
			Assert.fail("SerDe init exception");
		}
		Object row;
		try {
			row = serDe.deserialize(sample);
			Assert.assertNotNull("deserialized row was null", row);
		} catch (Exception e) {
			e.printStackTrace(System.err);
			Assert.fail("SerDe deserialize exception");
			return;
		}
		ReflectionStructObjectInspector oi;
		try {
			oi = (ReflectionStructObjectInspector) serDe.getObjectInspector();
		} catch (Exception e) {
			e.printStackTrace(System.err);
			Assert.fail("serDe.getObjectInspector exception");
			return;
		}
		List<? extends StructField> fieldRefs = oi.getAllStructFieldRefs();
		for (int i = 0; i < fieldRefs.size(); i++) {
			Object fieldData = oi.getStructFieldData(row, fieldRefs.get(i));
			String fieldName = fieldRefs.get(i).toString();
			fieldName = fieldName.substring(fieldName.lastIndexOf(".") + 1);
			System.out.println(String.format("%15s = %s", fieldName, fieldData == null ? "null" : fieldData.toString()));
		}

	}

}

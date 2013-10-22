import java.util.List;
import java.util.Properties;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ReflectionStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import com.snowplowanalytics.hive.serde.CfLogDeserializer;

public class CfLogDeserializerTest {
	private CfLogDeserializer serDe;
	private Configuration conf;
	
	@Before
	public void before() {
		try {
			serDe = new CfLogDeserializer();
		} catch (SerDeException e1) {
			e1.printStackTrace();
			Assert.fail("CfLogDeserializer constructor raised an exception");
			return;
		}
		conf = new Configuration();
		try {
			serDe.initialize(conf, new Properties());
		} catch (Exception e) {
			e.printStackTrace(System.err);
			Assert.fail("SerDe init exception");
		}
	}
	
	public void parseText(String lineToTest) {
		Object row;
		Text sample = new Text(lineToTest);
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

		System.out.println("------------------");
	}

	@Test
	public void testOldLogFormatVersion() {
		parseText("07/01/2012 01:13:11 FRA2 182 192.0.2.10 GET d111111abcdef8.cloudfront.net /view/my/file.html 200 www.displaymyfiles.com Mozilla/4.0%20(compatible;%20MSIE%205.0b1;%20Mac_PowerPC) - zip=98101 RefreshHit MRVMF7KydIvxMWfJIglgwHQwZsbG2IhRJ07sn9AkKUFSHS9EXAMPLE==");
		// "07/01/2012 01:13:12 LAX1 2390282 192.0.2.202 GET d111111abcdef8.cloudfront.net /soundtrack/happy.mp3 304 www.unknownsingers.com Mozilla/4.0%20(compatible;%20MSIE%207.0;%20Windows%20NT%205.1) a=b&c=d zip=50158 Hit xGN7KWpVEmB9Dp7ctcVFQC4E-nrcOcEKS3QyAez--06dV7TEXAMPLE=="
	}

	@Test
	public void testNewLogFormatVersion() {
		parseText("07/01/2012 01:13:11 FRA2 182 192.0.2.10 GET d111111abcdef8.cloudfront.net /view/my/file.html 200 www.displaymyfiles.com Mozilla/4.0%20(compatible;%20MSIE%205.0b1;%20Mac_PowerPC) - zip=98101 RefreshHit MRVMF7KydIvxMWfJIglgwHQwZsbG2IhRJ07sn9AkKUFSHS9EXAMPLE== www.x-host-header.com http 456");
	}

}

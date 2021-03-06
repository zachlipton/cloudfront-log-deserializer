/* 
 * Copyright (c) 2012 Orderly Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.hive.serde;

import java.nio.charset.CharacterCodingException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractDeserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * CfLogDeserializer reads CloudFront download distribution file access log data
 * into Hive.
 * 
 * For documentation please see the introductory README.md in the project root.
 */
public class CfLogDeserializer extends AbstractDeserializer {

	// -------------------------------------------------------------------------------------------------------------------
	// Initial setup
	// -------------------------------------------------------------------------------------------------------------------

	// Setup logging
	public static final Log LOG = LogFactory.getLog(CfLogDeserializer.class.getName());

	// Voodoo taken from Zemanta's S3LogDeserializer
	static {
		StackTraceElement[] sTrace = new Exception().getStackTrace();
		sTrace[0].getClassName();
	}

	// We'll initialize our object inspector below
	private ObjectInspector cachedObjectInspector;

	// For performance reasons we reuse the same object to deserialize all of
	// our rows
	private static final CfLogStruct cachedStruct = new CfLogStruct();

	// -------------------------------------------------------------------------------------------------------------------
	// Constructor & initializer
	// -------------------------------------------------------------------------------------------------------------------

	/**
	 * Empty constructor
	 */
	public CfLogDeserializer() throws SerDeException {
	}

	/**
	 * Initialize the CfLogDeserializer.
	 * 
	 * @param conf
	 *            System properties
	 * @param tbl
	 *            Table properties
	 * @throws SerDeException
	 *             For any exception during initialization
	 */
	@Override
	public void initialize(Configuration conf, Properties tbl) throws SerDeException {

		cachedObjectInspector = ObjectInspectorFactory.getReflectionObjectInspector(CfLogStruct.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
		LOG.debug(this.getClass().getName() + " initialized");
	}

	// -------------------------------------------------------------------------------------------------------------------
	// Deserializer
	// -------------------------------------------------------------------------------------------------------------------

	/**
	 * Deserialize an object out of a Writable blob. In most cases, the return
	 * value of this function will be constant since the function will reuse the
	 * returned object. If the client wants to keep a copy of the object, the
	 * client needs to clone the returned value by calling
	 * ObjectInspectorUtils.getStandardObject().
	 * 
	 */
	@Override
	public Object deserialize(Writable field) throws SerDeException {
		String row = null;
		if (field instanceof BytesWritable) {
			BytesWritable b = (BytesWritable) field;
			try {
				row = Text.decode(b.getBytes(), 0, b.getLength());
			} catch (CharacterCodingException e) {
				throw new SerDeException(e);
			}
		} else if (field instanceof Text) {
			row = field.toString();
		}
		try {
			// Construct and return the S3LogStruct from the row data
			cachedStruct.parse(row);
			return cachedStruct;
		} catch (ClassCastException e) {
			throw new SerDeException(this.getClass().getName() + " expects Text or BytesWritable", e);
		} catch (Exception e) {
			throw new SerDeException(e);
		}
	}

	// -------------------------------------------------------------------------------------------------------------------
	// Getters
	// -------------------------------------------------------------------------------------------------------------------

	/**
	 * Retrieve statistics for this SerDe. Returns null because we don't support
	 * statistics (yet).
	 * 
	 * @return The SerDe's statistics (null in this case)
	 */
	@Override
	public SerDeStats getSerDeStats() {
		return null;
	}

	/**
	 * Get the object inspector that can be used to navigate through the
	 * internal structure of the Object returned from deserialize(...).
	 * 
	 * @return The ObjectInspector for this Deserializer
	 * @throws SerDeException
	 *             For any exception during initialization
	 */
	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		return cachedObjectInspector;
	}
}

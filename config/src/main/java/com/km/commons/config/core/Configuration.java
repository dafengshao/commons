package com.km.commons.config.core;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import com.km.commons.config.util.TypeUtils;

/**
 * 配置文件工具
 */
public final class Configuration {
	private static Map<String, Object> configMap = new ConcurrentHashMap<String, Object>();
	// private static ReadWriteLock lock = new ReentrantReadWriteLock();
	// private static Lock readLock = lock.readLock();
	// private static Lock writeLock = lock.writeLock();

	private final static String booleanKey = "boolean_";
	private final static String intKey = "int_";
	private final static String longKey = "long_";
	private final static String bigDecimalKey = "bigDecimal_";
	private final static String shortKey = "short_";
	private final static String floatKey = "float_";
	private final static String doubleKey = "double_";
	private final static String dateKey = "date_";
	private final static String listKey = "list_";

	private Configuration() {
		super();
	}

	public static void put(String key, Object value) {
		put(key, value, true);
	}

	private static void put(String key, Object value, boolean removeOldValue) {
		if (value == null) {
			if (configMap.containsKey(key)) {
				configMap.remove(key);
				removeOldValue = true;
			} else {
				return;
			}
		} else {
			configMap.put(key, value);
		}
		if (!removeOldValue) {
			return;
		}
		configMap.remove(booleanKey + key);
		configMap.remove(intKey + key);
		configMap.remove(longKey + key);
		configMap.remove(bigDecimalKey + key);
		configMap.remove(shortKey + key);
		configMap.remove(floatKey + key);
		configMap.remove(doubleKey + key);
		configMap.remove(dateKey + key);
		configMap.remove(listKey + key);

	}

	public static String getString(String key) {
		Object obj = getObject(key);
		if (obj == null) {
			return null;
		}
		return obj.toString();
	}

	public static Object getObject(String key) {
		return configMap.get(key);
	}

	/** 为空时返回 false */
	public static Boolean getBooleanValue(String key) {
		String newKey = booleanKey + key;
		Object newValue = getObject(newKey);
		if (newValue != null) {
			return (Boolean) newValue;
		}
		String value = getString(key);
		Boolean defaul = TypeUtils.castToBoolean(value);
		if (defaul == null) {
			defaul = false;
		}
		put(newKey, defaul, false);
		return defaul;
	}

	public static Short getShortValue(String key) {
		String newKey = shortKey + key;
		Object newValue = getObject(newKey);
		if (newValue != null) {
			return (Short) newValue;
		}
		String value = getString(key);
		if (value == null) {
			return null;
		}
		Short defaul = null;
		defaul = TypeUtils.castToShort(value);
		put(newKey, defaul, false);
		return defaul;
	}

	public static Integer getIntValue(String key) {
		String newKey = intKey + key;
		Object newValue = getObject(newKey);
		if (newValue != null) {
			return (Integer) newValue;
		}
		String value = getString(key);
		if (value == null) {
			return null;
		}
		Integer defaul = null;
		defaul = TypeUtils.castToInt(value);
		put(newKey, defaul, false);
		return defaul;
	}

	public static Long getLongValue(String key) {
		String newKey = longKey + key;
		Object newValue = getObject(newKey);
		if (newValue != null) {
			return (Long) newValue;
		}
		String value = getString(key);
		if (value == null) {
			return null;
		}
		Long defaul = null;
		defaul = TypeUtils.castToLong(value);
		put(newKey, defaul, false);
		return defaul.longValue();
	}

	public static Float getFloatValue(String key) {
		String newKey = floatKey + key;
		Object newValue = getObject(newKey);
		if (newValue != null) {
			return (Float) newValue;
		}
		String value = getString(key);
		if (value == null) {
			return null;
		}
		Float defaul = null;
		defaul = TypeUtils.castToFloat(value);
		put(newKey, defaul, false);
		return defaul.floatValue();
	}

	public static Double getDoubleValue(String key) {
		String newKey = doubleKey + key;
		Object newValue = getObject(newKey);
		if (newValue != null) {
			return (Double) newValue;
		}
		String value = getString(key);
		if (value == null) {
			return null;
		}
		Double defaul = null;
		defaul = TypeUtils.castToDouble(value);
		put(newKey, defaul, false);
		return defaul.doubleValue();
	}

	public static BigDecimal getBigDecimal(String key) {
		String newKey = bigDecimalKey + key;
		Object newValue = getObject(newKey);
		if (newValue != null) {
			return (BigDecimal) newValue;
		}
		String value = getString(key);
		if (value == null) {
			return null;
		}
		BigDecimal defaul = null;
		defaul = TypeUtils.castToBigDecimal(value);
		put(newKey, defaul, false);
		return defaul;
	}

	public static Date getDate(String key) {
		String newKey = dateKey + key;
		Object newValue = getObject(newKey);
		if (newValue != null) {
			return (Date) newValue;
		}
		String value = getString(key);
		if (value == null) {
			return null;
		}
		Date defaul = null;
		defaul = TypeUtils.castToDate(value);
		put(newKey, defaul);
		return defaul;
	}

	@SuppressWarnings("unchecked")
	public static List<?> getList(String key) {
		String newKey = listKey + key;
		Object newValue = getObject(newKey);
		if (newValue != null) {
			return (List<String>) newValue;
		}
		String value = getString(key);
		if (value == null) {
			return null;
		}
		List<?> defaul = toArrayList(value);
		put(newKey, defaul);
		return defaul;
	}

	public static short getShortValue(String key, short defaultValue) {
		Short s = getShortValue(key);
		if (s == null) {
			return defaultValue;
		}
		return s.shortValue();
	}

	public static int getIntValue(String key, int defaultValue) {
		Integer s = getIntValue(key);
		if (s == null) {
			return defaultValue;
		}
		return s.intValue();
	}

	public static long getLongValue(String key, long defaultValue) {
		Long s = getLongValue(key);
		if (s == null) {
			return defaultValue;
		}
		return s.longValue();
	}

	public static float getFloatValue(String key, float defaultValue) {
		Float s = getFloatValue(key);
		if (s == null) {
			return defaultValue;
		}
		return s.floatValue();
	}

	public static double getDoubleValue(String key, double defaultValue) {
		Double s = getDoubleValue(key);
		if (s == null) {
			return defaultValue;
		}
		return s.doubleValue();
	}

	public static BigDecimal getBigDecimal(String key, BigDecimal defaultValue) {
		BigDecimal s = getBigDecimal(key);
		if (s == null) {
			return defaultValue;
		}
		return s;
	}

	public static Date getDate(String key, Date defaultDate) {
		Date s = getDate(key);
		if (s == null) {
			return defaultDate;
		}
		return s;
	}

	private static List<?> toArrayList(String value) {
		List<Object> list = null;
		String[] vals = value.split(",");
		for (String val : vals) {
			if (list == null) {
				list = new ArrayList<Object>();
			}
			list.add(TypeUtils.castToNormalObject(val));
		}
		return list;

	}

	public static void buildConfig(Properties props) {
		for (Object key : props.keySet()) {
			String keyStr = key.toString();
			String value = props.getProperty(keyStr);
			put(keyStr, value);
		}
	}

	public static void main(String[] args) {
		// put("hwf", 123);
		System.out.println(getIntValue("h312wf"));
		System.out.println(getIntValue("h312wf"));

	}
}

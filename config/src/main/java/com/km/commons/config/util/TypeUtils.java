package com.km.commons.config.util;

import java.lang.reflect.Array;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class TypeUtils {
  private TypeUtils() {

  }

  public static final String castToString(Object value) {
    if (value == null) {
      return null;
    }

    return value.toString();
  }

  public static final Byte castToByte(Object value) {
    if (value == null) {
      return null;
    }

    if (value instanceof Number) {
      return ((Number) value).byteValue();
    }

    if (value instanceof String) {
      String strVal = (String) value;
      if (strVal.length() == 0) {
        return null;
      }
      return Byte.parseByte(strVal);
    }

    throw new RuntimeException("can not cast to byte, value : " + value);
  }

  public static final Character castToChar(Object value) {
    if (value == null) {
      return null;
    }

    if (value instanceof Character) {
      return (Character) value;
    }

    if (value instanceof String) {
      String strVal = (String) value;

      if (strVal.length() == 0) {
        return null;
      }

      if (strVal.length() != 1) {
        throw new RuntimeException("can not cast to byte, value : " + value);
      }

      return strVal.charAt(0);
    }

    throw new RuntimeException("can not cast to byte, value : " + value);
  }

  public static final Short castToShort(Object value) {
    if (value == null) {
      return null;
    }

    if (value instanceof Number) {
      return ((Number) value).shortValue();
    }

    if (value instanceof String) {
      String strVal = (String) value;
      if (strVal.length() == 0) {
        return null;
      }
      return Short.parseShort(strVal);
    }

    throw new RuntimeException("can not cast to short, value : " + value);
  }

  public static final BigDecimal castToBigDecimal(Object value) {
    if (value == null) {
      return null;
    }

    if (value instanceof BigDecimal) {
      return (BigDecimal) value;
    }

    if (value instanceof BigInteger) {
      return new BigDecimal((BigInteger) value);
    }

    String strVal = value.toString();
    if (strVal.length() == 0) {
      return null;
    }

    return new BigDecimal(strVal);
  }

  public static final BigInteger castToBigInteger(Object value) {
    if (value == null) {
      return null;
    }

    if (value instanceof BigInteger) {
      return (BigInteger) value;
    }

    if (value instanceof Float || value instanceof Double) {
      return BigInteger.valueOf(((Number) value).longValue());
    }

    String strVal = value.toString();
    if (strVal.length() == 0) {
      return null;
    }

    return new BigInteger(strVal);
  }

  public static final Float castToFloat(Object value) {
    if (value == null) {
      return null;
    }

    if (value instanceof Number) {
      return ((Number) value).floatValue();
    }

    if (value instanceof String) {
      String strVal = value.toString();
      if (strVal.length() == 0) {
        return null;
      }

      return Float.parseFloat(strVal);
    }

    throw new RuntimeException("can not cast to float, value : " + value);
  }

  public static final Object castToNormalObject(String value) {
    Object o = null;
    try {
      o = castToInt(value);
      return o;
    } catch (RuntimeException e) {}
    try {
      o = castToDate(value);
      return o;
    } catch (RuntimeException e) {}
    try {
      o = castToDouble(value);
      return o;
    } catch (RuntimeException e) {}
    try {
      o = castToBoolean(value);
      return o;
    } catch (RuntimeException e) {}

    return value;
  }

  public static final Double castToDouble(Object value) {
    if (value == null) {
      return null;
    }

    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    }

    if (value instanceof String) {
      String strVal = value.toString();
      if (strVal.length() == 0) {
        return null;
      }
      return Double.parseDouble(strVal);
    }

    throw new RuntimeException("can not cast to double, value : " + value);
  }

  public static String DEFFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

  public static final Date castToDate(Object value) {
    if (value == null) {
      return null;
    }

    if (value instanceof Calendar) {
      return ((Calendar) value).getTime();
    }

    if (value instanceof Date) {
      return (Date) value;
    }

    long longValue = 0;

    if (value instanceof Number) {
      longValue = ((Number) value).longValue();
    }

    if (value instanceof String) {
      String strVal = (String) value;

      if (strVal.indexOf('-') != -1) {
        String format;
        if (strVal.length() == DEFFAULT_DATE_FORMAT.length()) {
          format = DEFFAULT_DATE_FORMAT;
        } else if (strVal.length() == 10) {
          format = "yyyy-MM-dd";
        } else if (strVal.length() == "yyyy-MM-dd HH:mm:ss".length()) {
          format = "yyyy-MM-dd HH:mm:ss";
        } else {
          format = "yyyy-MM-dd HH:mm:ss.SSS";
        }

        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        try {
          return (Date) dateFormat.parse(strVal);
        } catch (ParseException e) {
          throw new RuntimeException("can not cast to Date, value : " + strVal);
        }
      }

      if (strVal.length() == 0) {
        return null;
      }

      longValue = Long.parseLong(strVal);
    }

    if (longValue <= 0) {
      throw new RuntimeException("can not cast to Date, value : " + value);
    }

    return new Date(longValue);
  }

  public static final java.sql.Date castToSqlDate(Object value) {
    if (value == null) {
      return null;
    }

    if (value instanceof Calendar) {
      return new java.sql.Date(((Calendar) value).getTimeInMillis());
    }

    if (value instanceof java.sql.Date) {
      return (java.sql.Date) value;
    }

    if (value instanceof java.util.Date) {
      return new java.sql.Date(((java.util.Date) value).getTime());
    }

    long longValue = 0;

    if (value instanceof Number) {
      longValue = ((Number) value).longValue();
    }

    if (value instanceof String) {
      String strVal = (String) value;
      if (strVal.length() == 0) {
        return null;
      }

      longValue = Long.parseLong(strVal);
    }

    if (longValue <= 0) {
      throw new RuntimeException("can not cast to Date, value : " + value);
    }

    return new java.sql.Date(longValue);
  }

  public static final java.sql.Timestamp castToTimestamp(Object value) {
    if (value == null) {
      return null;
    }

    if (value instanceof Calendar) {
      return new java.sql.Timestamp(((Calendar) value).getTimeInMillis());
    }

    if (value instanceof java.sql.Timestamp) {
      return (java.sql.Timestamp) value;
    }

    if (value instanceof java.util.Date) {
      return new java.sql.Timestamp(((java.util.Date) value).getTime());
    }

    long longValue = 0;

    if (value instanceof Number) {
      longValue = ((Number) value).longValue();
    }

    if (value instanceof String) {
      String strVal = (String) value;
      if (strVal.length() == 0) {
        return null;
      }

      longValue = Long.parseLong(strVal);
    }

    if (longValue <= 0) {
      throw new RuntimeException("can not cast to Date, value : " + value);
    }

    return new java.sql.Timestamp(longValue);
  }

  public static final Long castToLong(Object value) {
    if (value == null) {
      return null;
    }

    if (value instanceof Number) {
      return ((Number) value).longValue();
    }

    if (value instanceof String) {
      String strVal = (String) value;
      if (strVal.length() == 0) {
        return null;
      }

      return Long.parseLong(strVal);
    }

    throw new RuntimeException("can not cast to long, value : " + value);
  }

  public static final Integer castToInt(Object value) {
    if (value == null) {
      return null;
    }

    if (value instanceof Integer) {
      return (Integer) value;
    }

    if (value instanceof Number) {
      return ((Number) value).intValue();
    }

    if (value instanceof String) {
      String strVal = (String) value;
      if (strVal.length() == 0) {
        return null;
      }

      return Integer.parseInt(strVal);
    }

    throw new RuntimeException("can not cast to int, value : " + value);
  }


  public static final Boolean castToBoolean(Object value) {
    if (value == null) {
      return null;
    }

    if (value instanceof Boolean) {
      return (Boolean) value;
    }

    if (value instanceof Number) {
      return ((Number) value).intValue() == 1;
    }

    if (value instanceof String) {
      String str = (String) value;
      if (str.length() == 0) {
        return null;
      }

      if ("true".equals(str)) {
        return Boolean.TRUE;
      }
      if ("false".equals(str)) {
        return Boolean.FALSE;
      }

      if ("1".equals(str)) {
        return Boolean.TRUE;
      }
    }

    throw new RuntimeException("can not cast to Boolean, value : " + value);
  }

  private static ConcurrentMap<String, Class<?>> mappings =
      new ConcurrentHashMap<String, Class<?>>();
  static {
    addBaseClassMappings();
  }

  public static void addClassMapping(String className, Class<?> clazz) {
    if (className == null) {
      className = clazz.getName();
    }

    mappings.put(className, clazz);
  }

  public static void addBaseClassMappings() {
    mappings.put("byte", byte.class);
    mappings.put("short", short.class);
    mappings.put("int", int.class);
    mappings.put("long", long.class);
    mappings.put("float", float.class);
    mappings.put("double", double.class);
    mappings.put("boolean", boolean.class);
    mappings.put("char", char.class);

    mappings.put("[byte", byte[].class);
    mappings.put("[short", short[].class);
    mappings.put("[int", int[].class);
    mappings.put("[long", long[].class);
    mappings.put("[float", float[].class);
    mappings.put("[double", double[].class);
    mappings.put("[boolean", boolean[].class);
    mappings.put("[char", char[].class);

    mappings.put(HashMap.class.getName(), HashMap.class);
  }

  public static void clearClassMapping() {
    mappings.clear();
    addBaseClassMappings();
  }

  public static Class<?> loadClass(String className) {
    if (className == null || className.length() == 0) {
      return null;
    }

    Class<?> clazz = mappings.get(className);

    if (clazz != null) {
      return clazz;
    }

    if (className.charAt(0) == '[') {
      Class<?> componentType = loadClass(className.substring(1));
      return Array.newInstance(componentType, 0).getClass();
    }

    if (className.startsWith("L") && className.endsWith(";")) {
      String newClassName = className.substring(1, className.length() - 1);
      return loadClass(newClassName);
    }

    try {
      clazz = Thread.currentThread().getContextClassLoader().loadClass(className);

      addClassMapping(className, clazz);

      return clazz;
    } catch (Throwable e) {
      // skip
    }

    try {
      clazz = Class.forName(className);

      addClassMapping(className, clazz);

      return clazz;
    } catch (Throwable e) {
      // skip
    }

    return clazz;
  }



  public static Class<?> getClass(Type type) {
    if (type.getClass() == Class.class) {
      return (Class<?>) type;
    }

    if (type instanceof ParameterizedType) {
      return getClass(((ParameterizedType) type).getRawType());
    }

    return Object.class;
  }



  public static String trim(String textContent) {
    if (textContent == null) {
      return null;
    }
    textContent = textContent.trim();
    while (textContent.startsWith("　")) {
      textContent = textContent.substring(1, textContent.length()).trim();
    }
    while (textContent.endsWith("　")) {
      textContent = textContent.substring(0, textContent.length() - 1).trim();
    }
    return textContent;
  }



  public static void main(String[] args) {
    String ab = "    abc    　";
    System.out.println(ab);
    System.out.println(trim(ab).length());

  }
}

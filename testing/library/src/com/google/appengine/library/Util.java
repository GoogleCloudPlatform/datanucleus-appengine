package com.google.appengine.library;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

final class Util {

  /**
   * Convenience class that returns "" for {@code null} value. This saves the
   * check against {@code null} at caller.
   */
  static final class NullToEmptyMapWrapper {
    final private Map<String, String> map;
  
    public NullToEmptyMapWrapper(Map<String, String> map) {
      this.map = map;
    }
  
    public String get(String k) {
      String v = map.get(k);
      return v == null ? "" : v;
    }

    public String put(String k, String v) {
      return map.put(k, v);
    }
}

  static final String nullSafeToString(Object obj) {
    return obj != null ? obj.toString() : "";
  }

  @SuppressWarnings("unchecked")
  static NullToEmptyMapWrapper wrapFormFields(HttpServletRequest req) {
    Map<String, String> formFieldsMap = new HashMap<String, String>();
    for (Enumeration<String> e = req.getParameterNames(); e.hasMoreElements();) {
      String fieldName = e.nextElement();
      String[] fieldValues = req.getParameterValues(fieldName);
      String oneValue = null; // holds the parameter as a concatenated string.
      if (fieldValues.length > 0) {
        oneValue = fieldValues[0];
      }
      for (int i = 1; i < fieldValues.length; i++) {
        oneValue += " " + fieldValues[i];
      }
      
      formFieldsMap.put(fieldName, oneValue);
    }
    return new NullToEmptyMapWrapper(formFieldsMap);
  }

}

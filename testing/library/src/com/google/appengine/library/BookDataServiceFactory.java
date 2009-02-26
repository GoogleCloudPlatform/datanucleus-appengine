// Copyright 2008 Google Inc. All Rights Reserved.

package com.google.appengine.library;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Query;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;

/**
 * @author kjin@google.com (Kevin Jin)
 * 
 */
public final class BookDataServiceFactory {

  // Do not instantiate.
  private BookDataServiceFactory() {
  }

  public static BookDataService getBookDataService() throws ServletException {
    try {
      return (BookDataService) Class.forName(dataServiceNameToClassMap.get(getServiceName()))
          .newInstance();
    } catch (InstantiationException e) {
      throw new ServletException(e);
    } catch (IllegalAccessException e) {
      throw new ServletException(e);
    } catch (ClassNotFoundException e) {
      throw new ServletException(e);
    }
  }

  public static void setServiceName(String serviceName) {
    BookDataServiceFactory.serviceName = serviceName;
    Entity e = fetchUserPrefs();
    e.setProperty("Data Service Name", serviceName);
    datastoreService.put(e);
  }

  private static Entity fetchUserPrefs() {
    Query query = new Query("UserPrefs");
    Entity e = datastoreService.prepare(query).asSingleEntity();
    if (e == null) {
      e = new Entity("UserPrefs");
    }
    return e;
  }

  /**
   * Gets the Data Service Name from Data Store if not initialized. Maybe it
   * should be done in {@code Servlet.init()}.
   * 
   * @return Data Service Name.
   */
  public static String getServiceName() {
    if (serviceName == null) {
      Entity e = fetchUserPrefs();
      serviceName = (String) e.getProperty("Data Service Name");
      if (serviceName == null || dataServiceNameToClassMap.get(serviceName) == null) {
        setServiceName("JPA");
      }
    }

    return serviceName;
  }

  // the native API is needed for finding the service name.
  private static final DatastoreService datastoreService =
      DatastoreServiceFactory.getDatastoreService();

  private static String serviceName;

  static final Map<String, String> dataServiceNameToClassMap = new HashMap<String, String>();
  static {
    dataServiceNameToClassMap.put("Native",
        "com.google.appengine.library.DataStoreBookDataService");
    dataServiceNameToClassMap.put("JDO",
        "com.google.appengine.library.JDOBookDataService");
    dataServiceNameToClassMap.put("JPA",
        "com.google.appengine.library.JPABookDataService");
  }
}

// Copyright 2008 Google Inc. All Rights Reserved.

package com.google.appengine.library;

/**
 * @author kjin@google.com (Kevin Jin)
 * 
 */
public interface BookDataService {

  void put(Book book);

  int countEntities(String jpqlQuery);

  Iterable<Book> asIterable(String jpqlQuery);

  void delete(Book book);

  Iterable<Book> asIterable(String jpqlQuery, int limit, int offset);

  /**
   * Closes the BookDataService. No further requests may be made on it.
   */
  void close();
  
}

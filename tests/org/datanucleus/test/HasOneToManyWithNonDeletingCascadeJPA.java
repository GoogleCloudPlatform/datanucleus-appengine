// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import java.util.Collection;

/**
 * @author Max Ross <maxr@google.com>
 */
public interface HasOneToManyWithNonDeletingCascadeJPA {

  Collection<Book> getBooks();

  void nullBooks();
}
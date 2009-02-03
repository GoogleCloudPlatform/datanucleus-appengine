// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import java.util.Collection;

/**
 * @author Max Ross <maxr@google.com>
 */
public interface HasOneToManyJPA {

  Collection<BidirectionalChildJPA> getBidirChildren();

  Collection<Book> getBooks();

  Collection<HasKeyPkJPA> getHasKeyPks();

  void setVal(String s);

  String getId();

  void nullBooks();

  void nullHasKeyPks();

  void nullBidirChildren();
}
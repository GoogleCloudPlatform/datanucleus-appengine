// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

/**
 * @author Max Ross <maxr@google.com>
 */
public interface BidirectionalChildJPA {
  HasOneToManyJPA getParent();
  void setParent(HasOneToManyJPA parent);
  String getId();
  String getChildVal();
  void setChildVal(String childVal);
}
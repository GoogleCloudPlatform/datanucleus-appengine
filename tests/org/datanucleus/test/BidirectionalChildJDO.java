// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

/**
 * @author Max Ross <maxr@google.com>
 */
public interface BidirectionalChildJDO {
  HasOneToManyJDO getParent();
  String getId();
  void setChildVal(String childVal);
  void setParent(HasOneToManyJDO parent);
  String getChildVal();
}

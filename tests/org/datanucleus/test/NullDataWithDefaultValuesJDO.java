// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import org.datanucleus.store.appengine.Utils;

import java.util.List;

import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.PrimaryKey;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.IdGeneratorStrategy;

/**
 * @author Max Ross <maxr@google.com>
 */
@PersistenceCapable(identityType = IdentityType.APPLICATION)
public class NullDataWithDefaultValuesJDO {

  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  private String id;

  private String string = "string";

  private String[] array = new String[] {"array"};

  private List<String> list = Utils.newArrayList();

  public String getId() {
    return id;
  }

  public String getString() {
    return string;
  }

  public String[] getArray() {
    return array;
  }

  public List<String> getList() {
    return list;
  }
}
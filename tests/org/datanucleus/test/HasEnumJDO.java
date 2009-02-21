// Copyright 2009 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import java.util.List;

import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

/**
 * @author Max Ross <maxr@google.com>
 */
@PersistenceCapable(identityType = IdentityType.APPLICATION)
public class HasEnumJDO {
  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  private Long key;

  @Persistent
  private MyEnum myEnum;
  @Persistent
  private MyEnum[] myEnumArray;
  @Persistent
  private List<MyEnum> myEnumList;


  public Long getKey() {
    return key;
  }

  public void setKey(Long key) {
    this.key = key;
  }

  public MyEnum getMyEnum() {
    return myEnum;
  }

  public void setMyEnum(MyEnum myEnum) {
    this.myEnum = myEnum;
  }

  public MyEnum[] getMyEnumArray() {
    return myEnumArray;
  }

  public void setMyEnumArray(MyEnum[] myEnumArray) {
    this.myEnumArray = myEnumArray;
  }

  public List<MyEnum> getMyEnumList() {
    return myEnumList;
  }

  public void setMyEnumList(List<MyEnum> myEnumList) {
    this.myEnumList = myEnumList;
  }

  public enum MyEnum {V1, V2};
}

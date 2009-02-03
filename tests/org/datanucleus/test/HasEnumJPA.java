// Copyright 2009 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import java.util.List;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

/**
 * @author Max Ross <maxr@google.com>
 */
@Entity
public class HasEnumJPA {
  @Id
  @GeneratedValue(strategy= GenerationType.IDENTITY)
  private String key;

  private MyEnum myEnum;
  private MyEnum[] myEnumArray;
  private List<MyEnum> myEnumList;


  public String getKey() {
    return key;
  }

  public void setKey(String key) {
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
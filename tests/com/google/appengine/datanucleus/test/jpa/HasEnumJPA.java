/**********************************************************************
Copyright (c) 2009 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**********************************************************************/
package com.google.appengine.datanucleus.test.jpa;

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
  private Long key;

  private MyEnum myEnum;
  private MyEnum[] myEnumArray;
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

  public enum MyEnum {V1, V2}
}
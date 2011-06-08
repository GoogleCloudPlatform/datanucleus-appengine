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
package org.datanucleus.test;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import java.util.List;
import java.util.Set;

/**
 * @author Max Ross <maxr@google.com>
 */
@Entity
public class NullDataJPA {

  @Id
  @GeneratedValue(strategy= GenerationType.IDENTITY)
  private Long id;

  private String string;

  private String[] array;

  private List<String> list;

  private Set<String> set;

  private List<Integer> integerList;

  private Set<Integer> integerSet;

  public Long getId() {
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

  public void setArray(String[] array) {
    this.array = array;
  }

  public void setList(List<String> list) {
    this.list = list;
  }

  public void setString(String s) {
    this.string = s;
  }

  public Set<String> getSet() {
    return set;
  }

  public void setSet(Set<String> set) {
    this.set = set;
  }

  public List<Integer> getIntegerList() {
    return integerList;
  }

  public void setIntegerList(List<Integer> integerList) {
    this.integerList = integerList;
  }

  public Set<Integer> getIntegerSet() {
    return integerSet;
  }

  public void setIntegerSet(Set<Integer> integerSet) {
    this.integerSet = integerSet;
  }
}
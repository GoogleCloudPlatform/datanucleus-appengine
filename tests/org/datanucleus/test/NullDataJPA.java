// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import java.util.List;
import java.util.Set;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

/**
 * @author Max Ross <maxr@google.com>
 */
@Entity
public class NullDataJPA {

  @Id
  @GeneratedValue(strategy= GenerationType.IDENTITY)
  private String id;

  private String string;

  private String[] array;

  private List<String> list;

  private Set<String> set;

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
}
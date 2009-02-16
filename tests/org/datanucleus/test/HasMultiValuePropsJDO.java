// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.TreeSet;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.SortedSet;
import java.util.LinkedHashSet;

import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

/**
 * @author Max Ross <maxr@google.com>
 */
@PersistenceCapable(identityType = IdentityType.APPLICATION)
public class HasMultiValuePropsJDO {
  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  private String id;

  @Persistent
  String str;

  @Persistent(defaultFetchGroup = "true")
  List<String> strList;

  @Persistent
  List<String> keyList;

  @Persistent(defaultFetchGroup = "true")
  Set<String> strSet;

  @Persistent(defaultFetchGroup = "true")
  HashSet<String> strHashSet;

  @Persistent(defaultFetchGroup = "true")
  TreeSet<String> strTreeSet;

  @Persistent(defaultFetchGroup = "true")
  ArrayList<String> strArrayList;

  @Persistent(defaultFetchGroup = "true")
  LinkedList<String> strLinkedList;

  @Persistent(defaultFetchGroup = "true")
  SortedSet<String> strSortedSet;

  @Persistent(defaultFetchGroup = "true")
  LinkedHashSet<String> strLinkedHashSet;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public List<String> getStrList() {
    return strList;
  }

  public void setStrList(List<String> strList) {
    this.strList = strList;
  }

  public List<String> getKeyList() {
    return keyList;
  }

  public void setKeyList(List<String> keyList) {
    this.keyList = keyList;
  }

  public String getStr() {
    return str;
  }

  public void setStr(String s) {
    this.str = s;
  }

  public Set<String> getStrSet() {
    return strSet;
  }

  public void setStrSet(Set<String> strSet) {
    this.strSet = strSet;
  }

  public TreeSet<String> getStrTreeSet() {
    return strTreeSet;
  }

  public void setStrTreeSet(TreeSet<String> strTreeSet) {
    this.strTreeSet = strTreeSet;
  }

  public ArrayList<String> getStrArrayList() {
    return strArrayList;
  }

  public void setStrArrayList(ArrayList<String> strArrayList) {
    this.strArrayList = strArrayList;
  }

  public LinkedList<String> getStrLinkedList() {
    return strLinkedList;
  }

  public void setStrLinkedList(LinkedList<String> strLinkedList) {
    this.strLinkedList = strLinkedList;
  }

  public HashSet<String> getStrHashSet() {
    return strHashSet;
  }

  public void setStrHashSet(HashSet<String> strHashSet) {
    this.strHashSet = strHashSet;
  }

  public SortedSet<String> getStrSortedSet() {
    return strSortedSet;
  }

  public void setStrSortedSet(SortedSet<String> strSortedSet) {
    this.strSortedSet = strSortedSet;
  }

  public LinkedHashSet<String> getStrLinkedHashSet() {
    return strLinkedHashSet;
  }

  public void setStrLinkedHashSet(LinkedHashSet<String> strLinkedHashSet) {
    this.strLinkedHashSet = strLinkedHashSet;
  }
}
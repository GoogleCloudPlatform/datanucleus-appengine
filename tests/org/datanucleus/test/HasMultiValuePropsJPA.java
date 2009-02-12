// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import java.util.List;
import java.util.HashSet;
import java.util.TreeSet;
import java.util.ArrayList;
import java.util.LinkedList;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.jdo.annotations.Persistent;

/**
 * @author Max Ross <maxr@google.com>
 */
@Entity
public class HasMultiValuePropsJPA {
  @Id
  @GeneratedValue(strategy= GenerationType.IDENTITY)
  private String id;

  List<String> strList;

  List<String> keyList;

  HashSet<String> strSet;

  TreeSet<String> strHashSet;

  TreeSet<String> strTreeSet;

  ArrayList<String> strArrayList;

  LinkedList<String> strLinkedList;

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

  public HashSet<String> getStrSet() {
    return strSet;
  }

  public void setStrSet(HashSet<String> strSet) {
    this.strSet = strSet;
  }

  public TreeSet<String> getStrHashSet() {
    return strHashSet;
  }

  public void setStrHashSet(TreeSet<String> strHashSet) {
    this.strHashSet = strHashSet;
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
}
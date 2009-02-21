// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

/**
 * @author Max Ross <maxr@google.com>
 */
@Entity
public class HasMultiValuePropsJPA {
  @Id
  @GeneratedValue(strategy= GenerationType.IDENTITY)
  private Long id;

  List<String> strList;

  List<String> keyList;

  Set<String> strSet;

  HashSet<String> strHashSet;

  TreeSet<String> strTreeSet;

  ArrayList<String> strArrayList;

  LinkedList<String> strLinkedList;

  SortedSet<String> strSortedSet;

  LinkedHashSet<String> strLinkedHashSet;

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
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
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

import java.util.ArrayList;
import java.util.Collection;
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

  Collection<Integer> intColl;

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

  public Collection<Integer> getIntColl() {
    return intColl;
  }

  public void setIntColl(Collection<Integer> intColl) {
    this.intColl = intColl;
  }
}
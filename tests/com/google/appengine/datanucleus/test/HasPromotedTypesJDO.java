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
package com.google.appengine.datanucleus.test;

import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

/**
 * @author Max Ross <maxr@google.com>
 */
@PersistenceCapable
public class HasPromotedTypesJDO {
  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  private Long id;

  @Persistent
  private int intVal;

  @Persistent
  private Integer integerVal;

  @Persistent
  private int[] intArray;

  @Persistent
  private Integer[] integerArray;

  @Persistent
  private List<Integer> intList;

  @Persistent
  private Set<Integer> intSet;

  @Persistent
  private LinkedList<Integer> intLinkedList;

  @Persistent
  private LinkedHashSet<Integer> intLinkedHashSet;

  @Persistent
  private long longPrimVal;

  @Persistent
  private Long longVal;

  @Persistent
  private long[] longPrimArray;

  @Persistent
  private Long[] longArray;

  @Persistent
  private List<Long> longList;

  @Persistent
  private Set<Long> longSet;

  @Persistent
  private LinkedList<Long> longLinkedList;

  @Persistent
  private LinkedHashSet<Long> longLinkedHashSet;

  public int getIntVal() {
    return intVal;
  }

  public void setIntVal(int intVal) {
    this.intVal = intVal;
  }

  public Integer getIntegerVal() {
    return integerVal;
  }

  public void setIntegerVal(Integer integerVal) {
    this.integerVal = integerVal;
  }

  public Long getId() {
    return id;
  }

  public int[] getIntArray() {
    return intArray;
  }

  public void setIntArray(int[] intArray) {
    this.intArray = intArray;
  }

  public Integer[] getIntegerArray() {
    return integerArray;
  }

  public void setIntegerArray(Integer[] integerArray) {
    this.integerArray = integerArray;
  }

  public long getLongPrimVal() {
    return longPrimVal;
  }

  public void setLongPrimVal(long longPrimVal) {
    this.longPrimVal = longPrimVal;
  }

  public Long getLongVal() {
    return longVal;
  }

  public void setLongVal(Long longVal) {
    this.longVal = longVal;
  }

  public long[] getLongPrimArray() {
    return longPrimArray;
  }

  public void setLongPrimArray(long[] longPrimArray) {
    this.longPrimArray = longPrimArray;
  }

  public Long[] getLongArray() {
    return longArray;
  }

  public void setLongArray(Long[] longArray) {
    this.longArray = longArray;
  }

  public List<Integer> getIntList() {
    return intList;
  }

  public void setIntList(List<Integer> intList) {
    this.intList = intList;
  }

  public Set<Integer> getIntSet() {
    return intSet;
  }

  public void setIntSet(Set<Integer> intSet) {
    this.intSet = intSet;
  }

  public List<Long> getLongList() {
    return longList;
  }

  public void setLongList(List<Long> longList) {
    this.longList = longList;
  }

  public Set<Long> getLongSet() {
    return longSet;
  }

  public void setLongSet(Set<Long> longSet) {
    this.longSet = longSet;
  }

  public LinkedList<Integer> getIntLinkedList() {
    return intLinkedList;
  }

  public void setIntLinkedList(LinkedList<Integer> intLinkedList) {
    this.intLinkedList = intLinkedList;
  }

  public LinkedHashSet<Integer> getIntLinkedHashSet() {
    return intLinkedHashSet;
  }

  public void setIntLinkedHashSet(LinkedHashSet<Integer> intLinkedHashSet) {
    this.intLinkedHashSet = intLinkedHashSet;
  }

  public LinkedList<Long> getLongLinkedList() {
    return longLinkedList;
  }

  public void setLongLinkedList(LinkedList<Long> longLinkedList) {
    this.longLinkedList = longLinkedList;
  }

  public LinkedHashSet<Long> getLongLinkedHashSet() {
    return longLinkedHashSet;
  }

  public void setLongLinkedHashSet(LinkedHashSet<Long> longLinkedHashSet) {
    this.longLinkedHashSet = longLinkedHashSet;
  }
}

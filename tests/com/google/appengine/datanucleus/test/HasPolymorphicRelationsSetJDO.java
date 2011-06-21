/**********************************************************************
 Copyright (c) 2011 Google Inc.

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

import com.google.appengine.api.datastore.Key;
import com.google.appengine.datanucleus.test.BidirectionalSuperclassTableChildSetJDO.BidirTop;
import com.google.appengine.datanucleus.test.BidirectionalSuperclassTableChildSetJDO.BidirTopLongPk;
import com.google.appengine.datanucleus.test.BidirectionalSuperclassTableChildSetJDO.BidirTopUnencodedStringPkJDO;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsJDO.HasOneToManyJDO;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsJDO.HasOneToManyKeyPkJDO;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsJDO.HasOneToManyLongPkJDO;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsJDO.HasOneToManyUnencodedStringPkJDO;
import com.google.appengine.datanucleus.test.UnidirectionalSuperclassTableChildJDO.UnidirTop;


import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import javax.jdo.annotations.Element;
import javax.jdo.annotations.Extension;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

public class HasPolymorphicRelationsSetJDO {
  
  @PersistenceCapable(detachable = "true")
  public static class HasOneToManySet implements HasOneToManyJDO {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value="true")
    private String id;
    
    private String val;

    @Persistent(mappedBy = "parent")
    @Element(dependent = "true")
    private Set<BidirTop> bidirChildren = new HashSet<BidirTop>();

    @Element(dependent = "true")
    private Set<UnidirTop> unidirChildren = new HashSet<UnidirTop>();
    
    public String getId() {
      return id;
    }

    public void addAtPosition(int i, BidirectionalSuperclassTableChildJDO.BidirTop bidir) {
      throw new UnsupportedOperationException();
    }

    public void addAtPosition(int i, UnidirectionalSuperclassTableChildJDO.UnidirTop unidir) {
      throw new UnsupportedOperationException();
    }

    public void addBidirChild(BidirectionalSuperclassTableChildJDO.BidirTop bidir) {
      this.bidirChildren.add((BidirTop)bidir);
    }

    public void addBidirChildAtPosition(BidirectionalSuperclassTableChildJDO.BidirTop bidir, int pos) {
      throw new UnsupportedOperationException();
    }

    public void addUnidirAtPosition(UnidirectionalSuperclassTableChildJDO.UnidirTop unidir, int pos) {
      throw new UnsupportedOperationException();
    }

    public void addUnidirChild(UnidirectionalSuperclassTableChildJDO.UnidirTop unidir) {
      this.unidirChildren.add(unidir);
    }
    public Set<? extends BidirectionalSuperclassTableChildJDO.BidirTop> getBidirChildren() {
      return new HashSet<BidirTop>(bidirChildren);
    }

    public Set<UnidirectionalSuperclassTableChildJDO.UnidirTop> getUnidirChildren() {
      return new HashSet<UnidirTop>(unidirChildren);
    }

    public String getVal() {
      return this.val;
    }

    public void removeBidirChildAtPosition(int i) {
      this.bidirChildren.remove(i);
    }

    public void removeBidirChildren(Collection<BidirectionalSuperclassTableChildJDO.BidirTop> bidirChildren) {
      this.bidirChildren.removeAll(bidirChildren);
    }

    public void removeUnidirChildAtPosition(int i) {
      this.unidirChildren.remove(i);
    }

    public void removeUnidirChildren(Collection<UnidirectionalSuperclassTableChildJDO.UnidirTop> unidirChildren) {
      this.unidirChildren.removeAll(unidirChildren);
    }

    @SuppressWarnings("unchecked")
    public void setBidirChildren(Collection<? extends BidirectionalSuperclassTableChildJDO.BidirTop> bidirChildren) {
      this.bidirChildren = (Set<BidirTop>)bidirChildren;
    }

    public void setUnidirChildren(Collection<UnidirectionalSuperclassTableChildJDO.UnidirTop> unidirChildren) {
      this.unidirChildren = (Set<UnidirectionalSuperclassTableChildJDO.UnidirTop>)unidirChildren;
    }

    public void setVal(String val) {
      this.val = val;
    }

    public void nullBidirChildren() {
      this.bidirChildren = null;
    }

    public void nullUnidirChildren() {
      this.unidirChildren = null;
    }

    public void clearBidirChildren() {
      this.bidirChildren.clear();
    }

    public void clearUnidirChildren() {
      this.unidirChildren.clear();
    }

  }
  
  @PersistenceCapable(detachable = "true")
  public static class HasOneToManyLongPkSet implements HasOneToManyLongPkJDO {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Long id;
    
    @Persistent(mappedBy = "parent", dependentElement = "true")
    @Element(dependent = "true")
    private Set<BidirTopLongPk> bidirChildren = new HashSet<BidirTopLongPk>();

    @Persistent(dependentElement = "true")
    @Element(dependent = "true")
    private Set<UnidirTop> unidirChildren = new HashSet<UnidirTop>();
    
    private String val;

    public void addBidirChild(BidirectionalSuperclassTableChildJDO.BidirTopLongPk child) {
      this.bidirChildren.add((BidirTopLongPk)child);
    }

    public void addUnidirChild(UnidirTop child) {
      this.unidirChildren.add(child);
    }

    public Collection<? extends BidirectionalSuperclassTableChildJDO.BidirTopLongPk> getBidirChildren() {
      return this.bidirChildren;
    }

    public Long getId() {
      return id;
    }

    public Collection<UnidirTop> getUnidirChildren() {
      return unidirChildren;
    }

    public void removeBidirChildren(Collection<? extends BidirectionalSuperclassTableChildJDO.BidirTopLongPk> children) {
      this.bidirChildren.removeAll(children);
    }

    public void removeUnidirChildren(Collection<UnidirTop> children) {
      this.unidirChildren.removeAll(children);
    }

    public void setVal(String val) {
      this.val = val;
    }

    public String getVal() {
      return val;
    }
    
  }

  @PersistenceCapable(detachable = "true")
  public static class HasOneToManyUnencodedStringPkSet implements HasOneToManyUnencodedStringPkJDO {
    
    @PrimaryKey
    private String id;
    
    @Persistent(mappedBy = "parent", dependentElement = "true")
    @Element(dependent = "true")
    private Set<BidirTopUnencodedStringPkJDO> bidirChildren = new HashSet<BidirTopUnencodedStringPkJDO>();

    @Persistent(dependentElement = "true")
    @Element(dependent = "true")
    private Set<UnidirectionalSuperclassTableChildJDO.UnidirTop> unidirChildren = new HashSet<UnidirectionalSuperclassTableChildJDO.UnidirTop>();

    public void addBidirChild(BidirectionalSuperclassTableChildJDO.BidirTopUnencodedStringPkJDO child) {
      this.bidirChildren.add((BidirTopUnencodedStringPkJDO)child);
    }

    public void addUnidirChild(UnidirTop unidir) {
      this.unidirChildren.add(unidir);
    }

    public Collection<? extends BidirectionalSuperclassTableChildJDO.BidirTopUnencodedStringPkJDO> getBidirChildren() {
      return this.bidirChildren;
    }

    public String getId() {
     return id;
    }

    public Collection<UnidirTop> getUnidirChildren() {
      return unidirChildren;
    }

    public boolean removeBidirChildren(
	Collection<? extends BidirectionalSuperclassTableChildJDO.BidirTopUnencodedStringPkJDO> children) {
      return this.bidirChildren.removeAll(children);
    }

    public boolean removeUnidirChildren(Collection<UnidirTop> children) {
      return this.unidirChildren.removeAll(children);
    }

    public void setId(String id) {
      this.id = id;
    }
    
  }

  @PersistenceCapable(detachable = "true")
  public static class HasOneToManyKeyPkSet implements HasOneToManyKeyPkJDO {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Key key;

    @Element(dependent = "true")
    private Set<UnidirTop> unidirChildren = new HashSet<UnidirTop>();

    public Key getId() {
      return key;
    }

    public void addUnidirChild(UnidirTop unidir) {
      this.unidirChildren.add(unidir);
    }

    public Collection<UnidirTop> getUnidirChildren() {
      return this.unidirChildren;
    }

  }
}

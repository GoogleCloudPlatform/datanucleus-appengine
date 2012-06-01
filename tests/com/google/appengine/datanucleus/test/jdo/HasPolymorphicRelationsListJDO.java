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
package com.google.appengine.datanucleus.test.jdo;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.datanucleus.Utils;
import com.google.appengine.datanucleus.test.jdo.BidirectionalSuperclassTableChildListJDO.BidirTop;
import com.google.appengine.datanucleus.test.jdo.BidirectionalSuperclassTableChildListJDO.BidirTopLongPk;
import com.google.appengine.datanucleus.test.jdo.BidirectionalSuperclassTableChildListJDO.BidirTopLongPkChildKey;
import com.google.appengine.datanucleus.test.jdo.BidirectionalSuperclassTableChildListJDO.BidirTopStringPk;
import com.google.appengine.datanucleus.test.jdo.BidirectionalSuperclassTableChildListJDO.BidirTopUnencodedStringPk;
import com.google.appengine.datanucleus.test.jdo.BidirectionalSuperclassTableChildListJDO.BidirTopUnencodedStringPkChildKey;
import com.google.appengine.datanucleus.test.jdo.HasPolymorphicRelationsJDO.HasOneToManyJDO;
import com.google.appengine.datanucleus.test.jdo.HasPolymorphicRelationsJDO.HasOneToManyKeyPkJDO;
import com.google.appengine.datanucleus.test.jdo.HasPolymorphicRelationsJDO.HasOneToManyLongPkJDO;
import com.google.appengine.datanucleus.test.jdo.HasPolymorphicRelationsJDO.HasOneToManyUnencodedStringPkJDO;
import com.google.appengine.datanucleus.test.jdo.HasPolymorphicRelationsJDO.HasOneToManyWithOrderByJDO;
import com.google.appengine.datanucleus.test.jdo.SubclassesJDO.CompleteTableParentNoChildStrategy;
import com.google.appengine.datanucleus.test.jdo.SubclassesJDO.CompleteTableParentWithCompleteTableChild;
import com.google.appengine.datanucleus.test.jdo.SubclassesJDO.CompleteTableParentWithNewTableChild;
import com.google.appengine.datanucleus.test.jdo.SubclassesJDO.CompleteTableParentWithSubclassTableChild;
import com.google.appengine.datanucleus.test.jdo.SubclassesJDO.NewTableParentWithCompleteTableChild;
import com.google.appengine.datanucleus.test.jdo.SubclassesJDO.NewTableParentWithNewTableChild;
import com.google.appengine.datanucleus.test.jdo.SubclassesJDO.NewTableParentWithSubclassTableChild;
import com.google.appengine.datanucleus.test.jdo.UnidirectionalSuperclassTableChildJDO.UnidirTop;
import com.google.appengine.datanucleus.test.jdo.UnidirectionalSuperclassTableChildJDO.UnidirTopEncodedStringPkSeparateNameField;
import com.google.appengine.datanucleus.test.jdo.UnidirectionalSuperclassTableChildJDO.UnidirTopWithIndexColumn;
import com.google.appengine.datanucleus.test.jdo.UnidirectionalSuperclassTableChildJDO.UnidirTopWithOverriddenIdColumn;


import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.jdo.annotations.Discriminator;
import javax.jdo.annotations.Element;
import javax.jdo.annotations.Extension;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.Order;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

public class HasPolymorphicRelationsListJDO {
  
  @PersistenceCapable(detachable = "true")
  public static class HasOneToManyList implements HasOneToManyJDO {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value="true")
    private String id;
    
    private String val;

    @Persistent(mappedBy = "parent")
    @Element(dependent = "true")
    private List<BidirTop> bidirChildren = new ArrayList<BidirTop>();

    @Element(dependent = "true")
    private List<UnidirTop> unidirChildren = new ArrayList<UnidirTop>();
    
    public String getId() {
      return id;
    }

    public void addAtPosition(int i, BidirectionalSuperclassTableChildJDO.BidirTop bidir) {
      this.bidirChildren.add(i, (BidirTop)bidir);
    }

    public void addAtPosition(int i, UnidirectionalSuperclassTableChildJDO.UnidirTop unidir) {
      this.unidirChildren.add(i, unidir);
    }

    public void addBidirChild(BidirectionalSuperclassTableChildJDO.BidirTop bidir) {
      this.bidirChildren.add((BidirTop)bidir);
    }

    public void addBidirChildAtPosition(BidirectionalSuperclassTableChildJDO.BidirTop bidir, int pos) {
      this.bidirChildren.set(pos, (BidirTop)bidir);
    }

    public void addUnidirAtPosition(UnidirectionalSuperclassTableChildJDO.UnidirTop unidir, int pos) {
      this.unidirChildren.set(pos, unidir);
    }

    public void addUnidirChild(UnidirectionalSuperclassTableChildJDO.UnidirTop unidir) {
      this.unidirChildren.add(unidir);
    }
    public List<? extends BidirectionalSuperclassTableChildJDO.BidirTop> getBidirChildren() {
      return bidirChildren;
    }

    public List<UnidirectionalSuperclassTableChildJDO.UnidirTop> getUnidirChildren() {
      return this.unidirChildren;
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
      this.bidirChildren = (List<BidirTop>)bidirChildren;
    }

    public void setUnidirChildren(Collection<UnidirectionalSuperclassTableChildJDO.UnidirTop> unidirChildren) {
      this.unidirChildren = (List<UnidirTop>)unidirChildren;
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
  public static class HasOneToManyListLongPk implements HasOneToManyLongPkJDO {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Long id;
    
    @Persistent(mappedBy = "parent")
    @Element(dependent = "true")
    private List<BidirTopLongPk> bidirChildren = new ArrayList<BidirTopLongPk>();

    @Element(dependent = "true")
    @Order(column = "unidirChildren_INTEGER_IDX_longpk")
    private List<UnidirTop> unidirChildren = new ArrayList<UnidirTop>();
    
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
  public static class HasOneToManyListUnencodedStringPk implements HasOneToManyUnencodedStringPkJDO {
    
    @PrimaryKey
    private String id;
    
    @Persistent(mappedBy = "parent")
    @Element(dependent = "true")
    private List<BidirTopUnencodedStringPk> bidirChildren = new ArrayList<BidirTopUnencodedStringPk>();

    @Element(dependent = "true")
    @Order(column = "unidirChildren_INTEGER_IDX_unencodedstringpk")
    private List<UnidirectionalSuperclassTableChildJDO.UnidirTop> unidirChildren = new ArrayList<UnidirectionalSuperclassTableChildJDO.UnidirTop>();

    public void addBidirChild(BidirectionalSuperclassTableChildJDO.BidirTopUnencodedStringPkJDO child) {
      this.bidirChildren.add((BidirTopUnencodedStringPk)child);
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
  public static class HasOneToManyListStringPk {
    
    @PrimaryKey
    private String id;
    
    private String val;
    
    @Persistent(mappedBy = "parent")
    @Element(dependent = "true")
    private List<BidirTopStringPk> bidirChildren = new ArrayList<BidirTopStringPk>();

    @Element(dependent = "true")
    private List<UnidirTop> unidirChildren = new ArrayList<UnidirTop>();

    public void addBidirChild(BidirTopStringPk child) {
      this.bidirChildren.add(child);
    }

    public void addUnidirChild(UnidirTop unidir) {
      this.unidirChildren.add(unidir);
    }

    public Collection<BidirTopStringPk> getBidirChildren() {
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

    public void setVal(String val) {
      this.val = val;
    }

    public String getVal() {
      return val;
    }
    
  }
  
  @PersistenceCapable(detachable = "true")
  public static class HasOneToManyListLongPkChildKeyPk {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Long id;

    public Long getId() {
      return id;
    }

    @Persistent(mappedBy = "parent")
    @Element(dependent = "true")
    private List<BidirTopLongPkChildKey> children = new ArrayList<BidirTopLongPkChildKey>();

    public List<BidirTopLongPkChildKey> getChildren() {
      return children;
    }

    public void setChildren(List<BidirTopLongPkChildKey> children) {
      this.children = children;
    }
  }
  
  @PersistenceCapable(detachable = "true")
  public static class HasOneToManyListUnencodedStringPkChildKey {
    @PrimaryKey
    private String id;

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    @Persistent(mappedBy = "parent")
    @Element(dependent = "true")
    private List<BidirTopUnencodedStringPkChildKey> children = new ArrayList<BidirTopUnencodedStringPkChildKey>();

    public List<BidirTopUnencodedStringPkChildKey> getChildren() {
      return children;
    }

    public void setChildren(List<BidirTopUnencodedStringPkChildKey> children) {
      this.children = children;
    }
  }

  @PersistenceCapable(detachable = "true")
  public static class HasOneToManyListKeyPk implements HasOneToManyKeyPkJDO {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Key key;

    @Element(dependent = "true")
    @Order(column = "unidirChildren_INTEGER_IDX_keypk")
    private List<UnidirTop> unidirChildren = new ArrayList<UnidirTop>();


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
  
  @PersistenceCapable(detachable = "true")
  public static class HasOneToManyChildAtMultipleLevels {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Key id;

    @Element(dependent = "true")
    @Order(extensions = @Extension(vendorName = "datanucleus", key="list-ordering", value="id"))
    private List<UnidirTop> unidirChildren = new ArrayList<UnidirTop>();

    @Persistent(dependent = "true")
    private HasOneToManyChildAtMultipleLevels child;

    public Key getId() {
      return id;
    }

    public List<UnidirTop> getUnidirChildren() {
      return unidirChildren;
    }

    public void setUnidirChildren(List<UnidirTop> unidirChildren) {
      this.unidirChildren = unidirChildren;
    }

    public HasOneToManyChildAtMultipleLevels getChild() {
      return child;
    }

    public void setChild(HasOneToManyChildAtMultipleLevels child) {
      this.child = child;
    }
  }
  
  @PersistenceCapable(detachable = "true")
  public static class HasOneToManyMultipleBidirChildren {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Long id;

    @Persistent(mappedBy = "parent")
    @Element(dependent = "true")
    private List<BidirChildTop1> child1 = Utils.newArrayList();

    @Persistent(mappedBy = "parent")
    @Element(dependent = "true")
    private List<BidirChildTop2> child2 = Utils.newArrayList();

    public Long getId() {
      return id;
    }

    public List<BidirChildTop1> getChild1() {
      return child1;
    }

    public void setChild1(List<BidirChildTop1> child1) {
      this.child1 = child1;
    }

    public List<BidirChildTop2> getChild2() {
      return child2;
    }

    public void setChild2(List<BidirChildTop2> child2) {
      this.child2 = child2;
    }

    @PersistenceCapable(detachable = "true")
    @Discriminator(column = "CLASS")
    public static class BidirChildTop1 {
      @PrimaryKey
      @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
      private Key id;

      @Persistent
      private HasOneToManyMultipleBidirChildren parent;

      public Key getId() {
        return id;
      }

      public void setId(Key id) {
        this.id = id;
      }

      public HasOneToManyMultipleBidirChildren getParent() {
        return parent;
      }
    }

    @PersistenceCapable(detachable = "true")
    public static class BidirChildMiddle1 extends BidirChildTop1 {
    }
    
    @PersistenceCapable(detachable = "true")
    public static class BidirChildBottom1 extends BidirChildMiddle1 {
    }

    @PersistenceCapable(detachable = "true")
    @Discriminator(column = "CLASS")
    public static class BidirChildTop2 {
      @PrimaryKey
      @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
      private Key id;

      @Persistent
      private HasOneToManyMultipleBidirChildren parent;

      public Key getId() {
        return id;
      }

      public void setId(Key id) {
        this.id = id;
      }

      public HasOneToManyMultipleBidirChildren getParent() {
        return parent;
      }
    }

    @PersistenceCapable(detachable = "true")
    public static class BidirChildMiddle2 extends BidirChildTop2 {
    }
    
    @PersistenceCapable(detachable = "true")
    public static class BidirChildBottom2 extends BidirChildMiddle2 {
    }
  }
  
  @PersistenceCapable(detachable = "true")
  public static class HasOneToManyWithSeparateNameFieldJDO {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Long id;

    @Persistent
    List<UnidirTopEncodedStringPkSeparateNameField> children = new ArrayList<UnidirTopEncodedStringPkSeparateNameField>();

    public Long getId() {
      return id;
    }

    public List<UnidirTopEncodedStringPkSeparateNameField> getChildren() {
      return children;
    }
  }
  
  @PersistenceCapable(detachable = "true")
  public static class HasOneToManyWithUnsupportedInheritanceList {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value="true")
    private String id;

    private List<CompleteTableParentWithCompleteTableChild> children1 = new ArrayList<CompleteTableParentWithCompleteTableChild>();
    private List<CompleteTableParentWithNewTableChild> children2 = new ArrayList<CompleteTableParentWithNewTableChild>();
    private List<CompleteTableParentWithSubclassTableChild> children3 = new ArrayList<CompleteTableParentWithSubclassTableChild>();
    private List<CompleteTableParentNoChildStrategy> children4 = new ArrayList<CompleteTableParentNoChildStrategy>();
    private List<NewTableParentWithCompleteTableChild> children5 = new ArrayList<NewTableParentWithCompleteTableChild>();
    private List<NewTableParentWithSubclassTableChild> children6 = new ArrayList<NewTableParentWithSubclassTableChild>();
    private List<NewTableParentWithNewTableChild> children7 = new ArrayList<NewTableParentWithNewTableChild>();

    
    public String getId() {
      return id;
    }


    public List<CompleteTableParentWithCompleteTableChild> getChildren1() {
      return children1;
    }


    public List<CompleteTableParentWithNewTableChild> getChildren2() {
      return children2;
    }


    public List<CompleteTableParentWithSubclassTableChild> getChildren3() {
      return children3;
    }


    public List<CompleteTableParentNoChildStrategy> getChildren4() {
      return children4;
    }


    public List<NewTableParentWithCompleteTableChild> getChildren5() {
      return children5;
    }


    public List<NewTableParentWithSubclassTableChild> getChildren6() {
      return children6;
    }


    public List<NewTableParentWithNewTableChild> getChildren7() {
      return children7;
    }
    
  }
  
  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class HasPolymorphicRelationsListWithOrderByJDO implements
      HasOneToManyWithOrderByJDO {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Long id;

    @Element(dependent = "true")
    @Order(extensions = @Extension(vendorName = "datanucleus", key="list-ordering", value="str DESC, name ASC"))
    private List<UnidirTop> unidirByStrAndName = new ArrayList<UnidirTop>();

    @Element(dependent = "true")
    @Order(extensions = @Extension(vendorName = "datanucleus", key="list-ordering", value="id DESC, str ASC"))
    private List<UnidirTop> unidirByIdAndStr = new ArrayList<UnidirTop>();

    @Element(dependent = "true")
    @Order(extensions = @Extension(vendorName = "datanucleus", key="list-ordering", value="str DESC, id ASC"))
    private List<UnidirTop> unidirByStrAndId = new ArrayList<UnidirTop>();

    @Order(mappedBy="index")
    private List<UnidirTopWithIndexColumn> unidirWithIndexColumn = new ArrayList<UnidirTopWithIndexColumn>();

    @Element(dependent = "true")
    @Order(extensions = @Extension(vendorName = "datanucleus", key="list-ordering", value="id ASC"))
    List<UnidirTopWithOverriddenIdColumn> unidirWithOverriddenIdColumn = new ArrayList<UnidirTopWithOverriddenIdColumn>();

    public Long getId() {
      return id;
    }

    public void setId(Long id) {
      this.id = id;
    }

    public List<UnidirTop> getUndirByStrAndName() {
      return unidirByStrAndName;
    }

    public List<UnidirTop> getUnidirByIdAndStr() {
      return unidirByIdAndStr;
    }

    public List<UnidirTop> getUnidirByStrAndId() {
      return unidirByStrAndId;
    }

    public List<UnidirTopWithIndexColumn> getUnidirWithIndexColumn() {
      return unidirWithIndexColumn;
    }

    public List<UnidirTopWithOverriddenIdColumn> getUnidirWithOverriddenIdColumn() {
      return unidirWithOverriddenIdColumn;
    }

  }

}

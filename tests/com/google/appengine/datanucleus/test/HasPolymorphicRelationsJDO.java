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
import com.google.appengine.datanucleus.test.BidirectionalSuperclassTableChildJDO.BidirTop;
import com.google.appengine.datanucleus.test.BidirectionalSuperclassTableChildJDO.BidirTopLongPk;
import com.google.appengine.datanucleus.test.BidirectionalSuperclassTableChildJDO.BidirTopUnencodedStringPkJDO;
import com.google.appengine.datanucleus.test.UnidirectionalSuperclassTableChildJDO.UnidirTop;
import com.google.appengine.datanucleus.test.UnidirectionalSuperclassTableChildJDO.UnidirTopWithIndexColumn;
import com.google.appengine.datanucleus.test.UnidirectionalSuperclassTableChildJDO.UnidirTopWithOverriddenIdColumn;


import java.util.Collection;
import java.util.List;

public interface HasPolymorphicRelationsJDO {
  
  public interface HasOneToManyJDO {
    String getId();
    Collection<? extends BidirTop> getBidirChildren();
    void addBidirChild(BidirTop bidir);
    Collection<UnidirTop> getUnidirChildren();
    void addUnidirChild(UnidirTop unidir);
    String getVal();
    void setVal(String val);
    void addBidirChildAtPosition(BidirTop bidir, int pos);
    void addUnidirAtPosition(UnidirTop unidir, int pos);
    void removeBidirChildAtPosition(int i);
    void removeUnidirChildAtPosition(int i);
    void removeBidirChildren(Collection<BidirTop> bidirChildren);
    void removeUnidirChildren(Collection<UnidirTop> unidirChildren);
    void addAtPosition(int i, BidirTop bidir);
    void addAtPosition(int i, UnidirTop unidir);
    void setBidirChildren(Collection<? extends BidirTop> childList);
    void setUnidirChildren(Collection<UnidirTop> childList);
    void nullUnidirChildren();
    void nullBidirChildren();
    void clearUnidirChildren();
    void clearBidirChildren();
  }

  public interface HasOneToManyKeyPkJDO {
    public Key getId();
    public void addUnidirChild(UnidirTop unidir);
    public Collection<UnidirTop> getUnidirChildren();
  }
  
  public interface HasOneToManyLongPkJDO {
    Long getId();
    void addUnidirChild(UnidirTop child);
    Collection<UnidirTop> getUnidirChildren();
    void addBidirChild(BidirTopLongPk child);
    Collection<? extends BidirTopLongPk> getBidirChildren();
    void removeUnidirChildren(Collection<UnidirTop> children);
    void removeBidirChildren(Collection<? extends BidirTopLongPk> children);
  }

  public interface HasOneToManyUnencodedStringPkJDO {
    void addUnidirChild(UnidirTop unidir);
    Collection<UnidirTop> getUnidirChildren();
    String getId();
    void setId(String s);
    void addBidirChild(BidirTopUnencodedStringPkJDO child);
    Collection<? extends BidirTopUnencodedStringPkJDO> getBidirChildren();
    boolean removeUnidirChildren(Collection<UnidirTop> children);
    boolean removeBidirChildren(Collection<? extends BidirTopUnencodedStringPkJDO> children);
  }
  
  public interface HasOneToManyWithOrderByJDO {
    Long getId();
    List<UnidirTop> getUndirByStrAndName();
    List<UnidirTop> getUnidirByIdAndStr();
    List<UnidirTop> getUnidirByStrAndId();
    List<UnidirTopWithIndexColumn> getUnidirWithIndexColumn();
    List<UnidirTopWithOverriddenIdColumn> getUnidirWithOverriddenIdColumn();
  }

}

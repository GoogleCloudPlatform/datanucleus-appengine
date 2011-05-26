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
 package org.datanucleus.test;

import org.datanucleus.test.HasPolymorphicRelationsJDO.HasOneToManyJDO;
import org.datanucleus.test.HasPolymorphicRelationsJDO.HasOneToManyLongPkJDO;
import org.datanucleus.test.HasPolymorphicRelationsJDO.HasOneToManyUnencodedStringPkJDO;

public interface BidirectionalSuperclassTableChildJDO {
  
  public enum BidirLevel {
    Top,
    Middle,
    Bottom
  }
  
  public interface BidirTop {
    void setChildVal(String string);
    String getChildVal();
    void setParent(HasOneToManyJDO parent);
    HasOneToManyJDO getParent();
    String getId();
    int getPropertyCount();
  }
  
  public interface BidirMiddle extends BidirTop {
    void setMiddleChildVal(Long middleChildVal);
    Long getMiddleChildVal();
  }
  
  public interface BidirBottom extends BidirMiddle {
    void setBottomChildVal(Double bottomChildVal);
    Double getBottomChildVal();
  }
  
  public interface BidirTopLongPk {
    void setChildVal(String string);
    String getChildVal();
    void setParent(HasOneToManyLongPkJDO parent);
    HasOneToManyLongPkJDO getParent();
    String getId();
  }

  public interface BidirMiddleLongPk extends BidirTopLongPk {
    
  }
  
  public interface BidirBottomLongPk extends BidirMiddleLongPk {
    
  }

  public interface BidirTopUnencodedStringPkJDO {
    HasOneToManyUnencodedStringPkJDO getParent();
    String getId();
    void setChildVal(String childVal);
    void setParent(HasOneToManyUnencodedStringPkJDO parent);
    String getChildVal();
  }
  
  public interface BidirMiddleUnencodedStringPkJDO extends BidirTopUnencodedStringPkJDO {
    
  }
  
  public interface BidirBottomUnencodedStringPkJDO extends BidirMiddleUnencodedStringPkJDO {
    
  }

}

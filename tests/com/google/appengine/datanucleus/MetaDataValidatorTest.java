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
package com.google.appengine.datanucleus;

import org.datanucleus.OMFContext;
import org.datanucleus.jdo.JDOPersistenceManagerFactory;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.MetaDataManager;

import com.google.appengine.datanucleus.test.Flight;

/**
 * @author Max Ross <maxr@google.com>
 */
public class MetaDataValidatorTest extends JDOTestCase {

  public void testIgnorableMapping_NoConfig() {
    setIgnorableMetaDataBehavior(null);
    OMFContext omfContext = ((JDOPersistenceManagerFactory)pmf).getOMFContext();
    MetaDataManager mdm = omfContext.getMetaDataManager();
    final String[] loggedMsg = {null};
    AbstractClassMetaData acmd =
        mdm.getMetaDataForClass(Flight.class, omfContext.getClassLoaderResolver(getClass().getClassLoader()));
    MetaDataValidator mdv = new MetaDataValidator(acmd, mdm, null) {
      @Override
      void warn(String msg) {
        loggedMsg[0] = msg;
      }
    };
    AbstractMemberMetaData ammd = acmd.getManagedMembers()[0];
    mdv.handleIgnorableMapping(ammd, "main msg", "warning only msg");
    assertTrue(loggedMsg[0].contains("main msg"));
    assertTrue(loggedMsg[0].contains("warning only msg"));
    assertTrue(loggedMsg[0].contains(MetaDataValidator.ADJUST_WARNING_MSG));
  }

  public void testIgnorableMapping_NoneConfig() {
    setIgnorableMetaDataBehavior(MetaDataValidator.IgnorableMetaDataBehavior.NONE.name());
    MetaDataManager mdm = ((JDOPersistenceManagerFactory)pmf).getOMFContext().getMetaDataManager();
    MetaDataValidator mdv = new MetaDataValidator(null, mdm, null) {
      @Override
      void warn(String msg) {
        fail("shouldn't have been called");
      }
    };
    mdv.handleIgnorableMapping(null, "main msg", "warning only msg");
  }

  public void testIgnorableMapping_WarningConfig() {
    setIgnorableMetaDataBehavior(MetaDataValidator.IgnorableMetaDataBehavior.WARN.name());
    OMFContext omfContext = ((JDOPersistenceManagerFactory)pmf).getOMFContext();
    MetaDataManager mdm = omfContext.getMetaDataManager();
    final String[] loggedMsg = {null};
    AbstractClassMetaData acmd =
        mdm.getMetaDataForClass(Flight.class, omfContext.getClassLoaderResolver(getClass().getClassLoader()));
    MetaDataValidator mdv = new MetaDataValidator(acmd, mdm, null) {
      @Override
      void warn(String msg) {
        loggedMsg[0] = msg;
      }
    };
    AbstractMemberMetaData ammd = acmd.getManagedMembers()[0];
    mdv.handleIgnorableMapping(ammd, "main msg", "warning only msg");
    assertTrue(loggedMsg[0].contains("main msg"));
    assertTrue(loggedMsg[0].contains("warning only msg"));
    assertTrue(loggedMsg[0].contains(MetaDataValidator.ADJUST_WARNING_MSG));
  }

  public void testIgnorableMapping_ErrorConfig() {
    setIgnorableMetaDataBehavior(MetaDataValidator.IgnorableMetaDataBehavior.ERROR.name());
    OMFContext omfContext = ((JDOPersistenceManagerFactory)pmf).getOMFContext();
    MetaDataManager mdm = omfContext.getMetaDataManager();
    AbstractClassMetaData acmd =
        mdm.getMetaDataForClass(Flight.class, omfContext.getClassLoaderResolver(getClass().getClassLoader()));
    MetaDataValidator mdv = new MetaDataValidator(acmd, mdm, null) {
      @Override
      void warn(String msg) {
        fail("shouldn't have been called");
      }
    };
    AbstractMemberMetaData ammd = acmd.getManagedMembers()[0];
    try {
      mdv.handleIgnorableMapping(ammd, "main msg", "warning only msg");
      fail("expected exception");
    } catch (MetaDataValidator.DatastoreMetaDataException dmde) {
      assertTrue(dmde.getMessage().contains("main msg"));
      assertFalse(dmde.getMessage().contains("warning only msg"));
      assertFalse(dmde.getMessage().contains(MetaDataValidator.ADJUST_WARNING_MSG));
    }
  }

  private void setIgnorableMetaDataBehavior(String val) {
    ((JDOPersistenceManagerFactory)pmf).getOMFContext().getPersistenceConfiguration().setProperty(
        "datanucleus.appengine.ignorableMetaDataBehavior", val);
  }
}
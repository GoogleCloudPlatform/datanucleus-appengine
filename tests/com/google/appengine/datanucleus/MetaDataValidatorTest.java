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

import org.datanucleus.NucleusContext;
import org.datanucleus.api.jdo.JDOPersistenceManagerFactory;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.InvalidMetaDataException;
import org.datanucleus.metadata.MetaDataManager;

import com.google.appengine.datanucleus.jdo.JDOTestCase;
import com.google.appengine.datanucleus.test.jdo.Flight;

/**
 * @author Max Ross <maxr@google.com>
 */
public class MetaDataValidatorTest extends JDOTestCase {

  public void testIgnorableMapping_NoConfig() {
    setIgnorableMetaDataBehavior(null);
    NucleusContext nucContext = ((JDOPersistenceManagerFactory)pmf).getNucleusContext();
    MetaDataManager mdm = nucContext.getMetaDataManager();
    final String[] loggedMsg = {null};
    AbstractClassMetaData acmd =
        mdm.getMetaDataForClass(Flight.class, nucContext.getClassLoaderResolver(getClass().getClassLoader()));
    MetaDataValidator mdv = new MetaDataValidator((DatastoreManager) nucContext.getStoreManager(), mdm, null) {
      @Override
      void warn(String msg) {
        loggedMsg[0] = msg;
      }
    };
    AbstractMemberMetaData ammd = acmd.getManagedMembers()[0];
    mdv.handleIgnorableMapping(acmd, ammd, "AppEngine.MetaData.TestMsg1", "warning only msg");
    assertTrue(loggedMsg[0].contains("main msg"));
    assertTrue(loggedMsg[0].contains("warning only msg"));
    assertTrue(loggedMsg[0].contains(MetaDataValidator.ADJUST_WARNING_MSG));
  }

  public void testIgnorableMapping_NoneConfig() {
    setIgnorableMetaDataBehavior(MetaDataValidator.IgnorableMetaDataBehavior.NONE.name());
    NucleusContext nucContext = ((JDOPersistenceManagerFactory)pmf).getNucleusContext();
    MetaDataManager mdm = nucContext.getMetaDataManager();
    MetaDataValidator mdv = new MetaDataValidator((DatastoreManager) nucContext.getStoreManager(), mdm, null) {
      @Override
      void warn(String msg) {
        fail("shouldn't have been called");
      }
    };
    mdv.handleIgnorableMapping(null, null, "AppEngine.MetaData.TestMsg1", "warning only msg");
  }

  public void testIgnorableMapping_WarningConfig() {
    setIgnorableMetaDataBehavior(MetaDataValidator.IgnorableMetaDataBehavior.WARN.name());
    NucleusContext nucContext = ((JDOPersistenceManagerFactory)pmf).getNucleusContext();
    MetaDataManager mdm = nucContext.getMetaDataManager();
    final String[] loggedMsg = {null};
    AbstractClassMetaData acmd =
        mdm.getMetaDataForClass(Flight.class, nucContext.getClassLoaderResolver(getClass().getClassLoader()));
    MetaDataValidator mdv = new MetaDataValidator((DatastoreManager) nucContext.getStoreManager(), mdm, null) {
      @Override
      void warn(String msg) {
        loggedMsg[0] = msg;
      }
    };
    AbstractMemberMetaData ammd = acmd.getManagedMembers()[0];
    mdv.handleIgnorableMapping(acmd, ammd, "AppEngine.MetaData.TestMsg1", "warning only msg");
    assertTrue(loggedMsg[0].contains("main msg"));
    assertTrue(loggedMsg[0].contains("warning only msg"));
    assertTrue(loggedMsg[0].contains(MetaDataValidator.ADJUST_WARNING_MSG));
  }

  public void testIgnorableMapping_ErrorConfig() {
    setIgnorableMetaDataBehavior(MetaDataValidator.IgnorableMetaDataBehavior.ERROR.name());
    NucleusContext nucContext = ((JDOPersistenceManagerFactory)pmf).getNucleusContext();
    MetaDataManager mdm = nucContext.getMetaDataManager();
    AbstractClassMetaData acmd =
        mdm.getMetaDataForClass(Flight.class, nucContext.getClassLoaderResolver(getClass().getClassLoader()));
    MetaDataValidator mdv = new MetaDataValidator((DatastoreManager) nucContext.getStoreManager(), mdm, null) {
      @Override
      void warn(String msg) {
        fail("shouldn't have been called");
      }
    };
    AbstractMemberMetaData ammd = acmd.getManagedMembers()[0];
    try {
      mdv.handleIgnorableMapping(acmd, ammd, "AppEngine.MetaData.TestMsg1", "warning only msg");
      fail("expected exception");
    } catch (InvalidMetaDataException imde) {
      assertTrue(imde.getMessage().contains("main msg"));
      assertFalse(imde.getMessage().contains("warning only msg"));
      assertFalse(imde.getMessage().contains(MetaDataValidator.ADJUST_WARNING_MSG));
    }
  }

  private void setIgnorableMetaDataBehavior(String val) {
    ((JDOPersistenceManagerFactory)pmf).getNucleusContext().getPersistenceConfiguration().setProperty(
        "datanucleus.appengine.ignorableMetaDataBehavior", val);
  }
}
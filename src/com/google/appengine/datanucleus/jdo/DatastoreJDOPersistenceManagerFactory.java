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
package com.google.appengine.datanucleus.jdo;

import org.datanucleus.api.jdo.JDOPersistenceManager;
import org.datanucleus.api.jdo.JDOPersistenceManagerFactory;

import java.util.Map;

/**
 * Extension to {@link JDOPersistenceManagerFactory} that allows us to
 * instantiate instances of {@link DatastoreJDOPersistenceManager} instead of
 * {@link JDOPersistenceManager}.
 *
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreJDOPersistenceManagerFactory extends JDOPersistenceManagerFactory {

  public DatastoreJDOPersistenceManagerFactory(Map props) {
    super(props);
  }

  @Override  
  protected JDOPersistenceManager newPM(
      JDOPersistenceManagerFactory jdoPersistenceManagerFactory, String userName, String password) {
    return new DatastoreJDOPersistenceManager(jdoPersistenceManagerFactory, userName, password);
  }
}

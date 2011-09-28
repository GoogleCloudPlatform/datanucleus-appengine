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
package com.google.appengine.datanucleus;

import org.datanucleus.util.CommandLine;
import org.datanucleus.util.StringUtils;

/**
 * Migration tool for moving from GAE v1 StorageVersion (children identified by parent key) to
 * GAE v2 StorageVersion (child keys stored in parents).
 */
public class Migrator {

  public static void main(String[] args) {
    CommandLine cmd = new CommandLine();
    cmd.addOption("pu", "persistenceUnit", "<persistence-unit>", 
      "name of the persistence unit to handle the schema for");
    cmd.addOption("pmf", "pmfName", "<pmf-name>",
      "name of the PMF to handle the schema for");
    cmd.parse(args);

    // Remaining command line args are filenames (class files, metadata files)
    String[] filenames = cmd.getDefaultArgs();

    String persistenceUnitName = null;
    if (cmd.hasOption("pu"))
    {
        persistenceUnitName = cmd.getOptionArg("pu");
    }

    System.out.println("GAE Migrator for persistence-unit=" + persistenceUnitName + " migrating the following classes : " +
        StringUtils.objectArrayToString(filenames));
    // TODO Identify classes to be migrated
    // TODO Read in Entities one-by-one using low-level API
    // TODO add field in parent with child key(s) and PUT the entity
  }
}

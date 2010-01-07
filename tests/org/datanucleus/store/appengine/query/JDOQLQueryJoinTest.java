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
package org.datanucleus.store.appengine.query;

import org.datanucleus.store.appengine.JDOTestCase;
import org.datanucleus.test.JoinsJDO.Course;
import org.datanucleus.test.JoinsJDO.Major;
import org.datanucleus.test.JoinsJDO.Student;
import static org.datanucleus.test.JoinsJDO.newCourse;
import static org.datanucleus.test.JoinsJDO.newMajor;
import static org.datanucleus.test.JoinsJDO.newStudent;

import java.util.Collections;

import javax.jdo.Query;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOQLQueryJoinTest extends JDOTestCase {

  public void testJoinOnOneToMany_Simple() {
    Course course1 = newCourse("Biology");
    makePersistentInTxn(course1, TXN_START_END);
    Course course2 = newCourse("Not Biology");
    makePersistentInTxn(course2, TXN_START_END);
    Student student = newStudent(10, course1, course2);
    makePersistentInTxn(student, TXN_START_END);
    beginTxn();
    Query q = pm.newQuery(
        "select from " + Student.class.getName() + " where "
        + "courses.contains(c) && c.department == 'Biology' && "
        + "grade == 10");
    q.declareVariables(Course.class.getName() + " c");
    assertEquals(Collections.singletonList(student), q.execute());
    commitTxn();
  }

  public void testJoinOnOneToMany_LegalOrderBy() {
    Course course1 = newCourse("Biology");
    makePersistentInTxn(course1, TXN_START_END);
    Course course2 = newCourse("Not Biology");
    makePersistentInTxn(course2, TXN_START_END);
    Student student = newStudent(10, course1, course2);
    makePersistentInTxn(student, TXN_START_END);
    beginTxn();
    Query q = pm.newQuery(
        "select from " + Student.class.getName() + " where "
        + "courses.contains(c) && c.department == 'Biology' && "
        + "grade == 10 order by courses asc");
    q.declareVariables(Course.class.getName() + " c");
    assertEquals(Collections.singletonList(student), q.execute());
    commitTxn();
  }

  public void testJoinOnOneToMany_Offset() {
    Course course1 = newCourse("Biology");
    makePersistentInTxn(course1, TXN_START_END);
    Course course2 = newCourse("Not Biology");
    makePersistentInTxn(course2, TXN_START_END);
    Student student = newStudent(10, course1, course2);
    makePersistentInTxn(student, TXN_START_END);
    Student student2 = newStudent(11, course1, course2);
    makePersistentInTxn(student2, TXN_START_END);
    Student student3 = newStudent(10, course1, course2);
    makePersistentInTxn(student3, TXN_START_END);
    beginTxn();
    Query q = pm.newQuery(
        "select from " + Student.class.getName() + " where "
        + "courses.contains(c) && c.department == 'Biology' && "
        + "grade == 10");
    q.declareVariables(Course.class.getName() + " c");
    q.setRange(1, Long.MAX_VALUE);
    assertEquals(Collections.singletonList(student3), q.execute());
    q.setRange(2, Long.MAX_VALUE);
    assertEquals(Collections.emptyList(), q.execute());
    commitTxn();
  }

  public void testJoinOnOneToMany_Limit() {
    Course course1 = newCourse("Biology");
    makePersistentInTxn(course1, TXN_START_END);
    Course course2 = newCourse("Not Biology");
    makePersistentInTxn(course2, TXN_START_END);
    Student student = newStudent(10, course1, course2);
    makePersistentInTxn(student, TXN_START_END);
    Student student2 = newStudent(11, course1, course2);
    makePersistentInTxn(student2, TXN_START_END);
    Student student3 = newStudent(10, course1, course2);
    makePersistentInTxn(student3, TXN_START_END);
    beginTxn();
    Query q = pm.newQuery(
        "select from " + Student.class.getName() + " where "
        + "courses.contains(c) && c.department == 'Biology' && "
        + "grade == 10");
    q.declareVariables(Course.class.getName() + " c");
    q.setRange(0, 1);
    assertEquals(Collections.singletonList(student), q.execute());
    q.setRange(0, 0);
    assertEquals(Collections.emptyList(), q.execute());
    commitTxn();
  }

  public void testJoinOnOneToOne_Simple() {
    Major major1 = newMajor("Liberal Arts");
    makePersistentInTxn(major1, TXN_START_END);
    Major major2 = newMajor("Engineering");
    makePersistentInTxn(major2, TXN_START_END);
    Student student1 = newStudent(10, major1);
    Student student2 = newStudent(10, major2);
    makePersistentInTxn(student1, TXN_START_END);
    makePersistentInTxn(student2, TXN_START_END);
    beginTxn();
    Query q = pm.newQuery(
        "select from " + Student.class.getName() + " where "
        + "major == m && m.school == 'Liberal Arts' && "
        + "grade == 10");
    q.declareVariables(Major.class.getName() + " m");
    assertEquals(Collections.singletonList(student1), q.execute());
    commitTxn();
  }

  public void testJoinOnOneToMany_Illegal() {
    beginTxn();
    // join condition can't be >
    Query q = pm.newQuery(
        "select from " + Student.class.getName() + " where "
        + "courses > c && c.department == 'Biology' && "
        + "grade == 10");
    q.declareVariables(Course.class.getName() + " c");
    try {
      q.execute();
      fail("expected exception");
    } catch (DatastoreQuery.UnsupportedDatastoreFeatureException udfe) {
      // good
    }

    // all filters on parent must be equality filters
    q = pm.newQuery(
        "select from " + Student.class.getName() + " where "
        + "courses.contains(c) && c.department == 'Biology' && "
        + "grade > 10");
    q.declareVariables(Course.class.getName() + " c");
    try {
      q.execute();
      fail("expected exception");
    } catch (DatastoreQuery.UnsupportedDatastoreFeatureException udfe) {
      // good
    }

    // all filters on child must be equality filters
    q = pm.newQuery(
        "select from " + Student.class.getName() + " where "
        + "courses.contains(c) && c.department > 'Biology' && "
        + "grade == 10");
    q.declareVariables(Course.class.getName() + " c");
    try {
      q.execute();
      fail("expected exception");
    } catch (DatastoreQuery.UnsupportedDatastoreFeatureException udfe) {
      // good
    }

    // sort on parent can only be by join column in asc order
    q = pm.newQuery(
        "select from " + Student.class.getName() + " where "
        + "courses.contains(c) && c.department == 'Biology' && "
        + "grade == 10 order by grade");
    q.declareVariables(Course.class.getName() + " c");
    try {
      q.execute();
      fail("expected exception");
    } catch (DatastoreQuery.UnsupportedDatastoreFeatureException udfe) {
      // good
    }

    // sort is by the join column but in the wrong order
    q = pm.newQuery(
        "select from " + Student.class.getName() + " where "
        + "courses.contains(c) && c.department == 'Biology' && "
        + "grade == 10 order by courses desc");
    q.declareVariables(Course.class.getName() + " c");
    try {
      q.execute();
      fail("expected exception");
    } catch (DatastoreQuery.UnsupportedDatastoreFeatureException udfe) {
      // good
    }

    // can't sort by child property
    q = pm.newQuery(
        "select from " + Student.class.getName() + " where "
        + "courses.contains(c) && c.department == 'Biology' && "
        + "grade == 10 order by c.department");
    q.declareVariables(Course.class.getName() + " c");
    try {
      q.execute();
      fail("expected exception");
    } catch (DatastoreQuery.UnsupportedDatastoreFeatureException udfe) {
      // good
    }
    commitTxn();
  }
}

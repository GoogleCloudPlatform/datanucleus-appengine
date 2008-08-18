// Copyright 2008 Google Inc. All Rights Reserved.
package com.google.appengine.helloorm;

import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * @author Max Ross <maxr@google.com>
 */
public class UpdatePersistenceStandard extends HttpServlet {

  private ServletConfig config;
  public void init(ServletConfig config) {
    this.config = config;
  }

  public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    PersistenceStandard ps = PersistenceStandard.valueOf(req.getParameter("persistenceStandard"));
    PersistenceStandard.set(ps);
    resp.sendRedirect("/");
  }
}

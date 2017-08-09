/*
 *
 *  *  Copyright 2010-2017 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.test;

import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;
import java.util.Iterator;
import java.util.Random;

/**
 * @author Sergey Sitnikov
 */
public class Issue7610Test {

  private OrientGraphFactory factory;

  @Before
  public void before() {
    //factory = new OrientGraphFactory("remote:localhost/plocalgraph01", "root", "q"); // for remote
    factory = new OrientGraphFactory("plocal:plocalgraph01", "admin", "admin"); // for embedded
  }

  @Test
  public void init() { // run once to init the database
    OrientGraphNoTx graphNoTx = factory.getNoTx();
    graphNoTx.command(new OCommandSQL("ALTER DATABASE DATETIMEFORMAT \"yyyyMMddHHmmss\"\n")).execute();
    graphNoTx.command(new OCommandSQL("CREATE CLASS Person EXTENDS V\n")).execute();
    graphNoTx.command(new OCommandSQL("CREATE PROPERTY Person.memberId STRING (MANDATORY TRUE)\n")).execute();
    graphNoTx.command(new OCommandSQL("CREATE INDEX Person.memberId ON Person (memberId) UNIQUE\n")).execute();
    graphNoTx.command(new OCommandSQL("CREATE CLASS FollowEdge EXTENDS E\n")).execute();
    graphNoTx.command(new OCommandSQL("CREATE PROPERTY FollowEdge.fName STRING (MANDATORY TRUE)\n")).execute();
    graphNoTx.command(new OCommandSQL("CREATE PROPERTY FollowEdge.createdDate STRING (MANDATORY TRUE)\n")).execute();
    graphNoTx.command(new OCommandSQL("CREATE INDEX FollowEdge.fName ON FollowEdge (fName) UNIQUE\n")).execute();
    graphNoTx.shutdown();
  }

  @Test
  public void benchmark() { // run to reproduce the issue
    final Random random = new Random();
    final int queries = 100000;

    final long start = System.currentTimeMillis();
    for (int i = 0; i < queries; ++i)
      addFollowRelation(Integer.toString(random.nextInt(5000000)), Integer.toString(random.nextInt(5000000)));
    final long end = System.currentTimeMillis();

    System.out.println(queries / ((end - start) / 1000.0));
  }

  public void addFollowRelation(String fromMemberId, String toMemberId) {
    OrientGraph graph = factory.getTx();
    try {
      boolean hasFollowRelation = isEdgeExisted(graph, fromMemberId, toMemberId);
      // If relation is not existed, insert it
      if (!hasFollowRelation) {
        OrientVertex fromV = createMemberVertex(graph, fromMemberId);
        OrientVertex toV = createMemberVertex(graph, toMemberId);
        fromV.addEdge(null, toV, "FollowEdge", null, "fName", fromMemberId + ">" + toMemberId, "createdDate",
            (new Date()).getTime());
        graph.commit();
      }
    } finally {
      graph.shutdown();
    }
  }

  private OrientVertex createMemberVertex(OrientGraph graph, String memberId) {
    OrientVertex v = retrieveMemberVertex(graph, memberId);
    if (null == v) {
      v = graph.addVertex("class:Person", "memberId", memberId);
      graph.commit();
    }
    return v;
  }

  private OrientVertex retrieveMemberVertex(OrientGraph graph, String memberId) {
    OrientVertex v = null;
    Iterator<Vertex> it = graph.query().has("memberId", memberId).vertices().iterator();
    if (it.hasNext()) {
      v = (OrientVertex) it.next();
    }
    return v;
  }

  private boolean isEdgeExisted(OrientGraph graph, String fromMemberId, String toMemberId) {
    boolean result = false;
    Iterable<Edge> it = graph.getEdges("FollowEdge.fName", fromMemberId + ">" + toMemberId);
    if (it.iterator().hasNext()) {
      result = true;
    }
    return result;
  }

}

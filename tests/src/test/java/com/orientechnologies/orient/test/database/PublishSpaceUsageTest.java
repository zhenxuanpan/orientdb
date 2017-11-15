package com.orientechnologies.orient.test.database;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import org.testng.annotations.Test;

@Test
public class PublishSpaceUsageTest {
  public void testPublishSpace() {
    final OrientDB orientDB = new OrientDB("plocal:./target", OrientDBConfig.defaultConfig());
    ODatabaseSession session = orientDB.open("GratefulDeadConcerts", "admin", "admin");
    OAbstractPaginatedStorage storage = (OAbstractPaginatedStorage) ((ODatabaseDocumentInternal) session).getStorage();
    storage.publishSBTreeSpaceUsage();
    session.close();
    orientDB.close();
  }
}
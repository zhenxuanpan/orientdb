package com.orientechnologies.orient.core.db.record.ridbag.embedded;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.orientechnologies.common.util.OResettable;
import com.orientechnologies.common.util.OSizeable;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.db.record.ridbag.embedded.OEmbeddedRidBag.Tombstone;
import com.orientechnologies.orient.core.record.ORecordInternal;

final class OEmbeddedRidBagEntriesIterator implements Iterator<OIdentifiable>, OResettable, OSizeable {
  /**
   * 
   */
  private final OEmbeddedRidBag ridbag;
  private final boolean convertToRecord;
  private int           currentIndex = -1;
  private int           nextIndex    = -1;
  private boolean       currentRemoved;

  OEmbeddedRidBagEntriesIterator(OEmbeddedRidBag oEmbeddedRidBag, boolean convertToRecord) {
    ridbag = oEmbeddedRidBag;
    reset();
    this.convertToRecord = convertToRecord;
  }

  @Override
  public boolean hasNext() {
    // we may remove items in ridbag during iteration so we need to be sure that pointed item is not removed.
    if (nextIndex > -1) {
      if (ridbag.entries[nextIndex] instanceof OIdentifiable)
        return true;

      nextIndex = nextIndex();
    }

    return nextIndex > -1;
  }

  @Override
  public OIdentifiable next() {
    currentRemoved = false;

    currentIndex = nextIndex;
    if (currentIndex == -1)
      throw new NoSuchElementException();

    Object nextValue = ridbag.entries[currentIndex];

    // we may remove items in ridbag during iteration so we need to be sure that pointed item is not removed.
    if (!(nextValue instanceof OIdentifiable)) {
      nextIndex = nextIndex();

      currentIndex = nextIndex;
      if (currentIndex == -1)
        throw new NoSuchElementException();

      nextValue = ridbag.entries[currentIndex];
    }

    nextIndex = nextIndex();

    final OIdentifiable identifiable = (OIdentifiable) nextValue;
    if (convertToRecord)
      return identifiable.getRecord();

    return identifiable;
  }

  @Override
  public void remove() {
    if (currentRemoved)
      throw new IllegalStateException("Current element has already been removed");

    if (currentIndex == -1)
      throw new IllegalStateException("Next method was not called for given iterator");

    currentRemoved = true;

    final OIdentifiable nextValue = (OIdentifiable) ridbag.entries[currentIndex];
    ridbag.entries[currentIndex] = Tombstone.TOMBSTONE;

    ridbag.size--;
    if (ridbag.owner != null)
      ORecordInternal.unTrack(ridbag.owner, nextValue);

    ridbag.fireCollectionChangedEvent(new OMultiValueChangeEvent<OIdentifiable, OIdentifiable>(OMultiValueChangeEvent.OChangeType.REMOVE,
        nextValue, null, nextValue));
  }

  @Override
  public void reset() {
    currentIndex = -1;
    nextIndex = -1;
    currentRemoved = false;

    nextIndex = nextIndex();
  }

  @Override
  public int size() {
    return ridbag.size;
  }

  private int nextIndex() {
    for (int i = currentIndex + 1; i < ridbag.entriesLength; i++) {
      Object entry = ridbag.entries[i];
      if (entry instanceof OIdentifiable)
        return i;
    }

    return -1;
  }
}
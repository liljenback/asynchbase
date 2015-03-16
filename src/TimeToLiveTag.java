package org.hbase.async;

public class TimeToLiveTag extends Tag {

  public TimeToLiveTag(long ttl_in_milliseconds) {
    super(TTL_TAG_TYPE, Bytes.fromLong(ttl_in_milliseconds));
  }

  public static void main(String args[]) {
    Tag t1 = new TimeToLiveTag((long)10000);
    System.out.println(Bytes.pretty(t1.getBackingByteArray()));
    Tag.checkTag(t1.getBackingByteArray());
  }

}

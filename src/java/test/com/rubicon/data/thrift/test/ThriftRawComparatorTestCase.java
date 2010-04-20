package com.rubicon.data.thrift.test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.thrift.TBase;

import com.rubicon.data.thrift.ThriftRawComparator;
import com.rubicon.data.thrift.ThriftCompactSerializer;
import com.rubicon.data.thrift.UserProfile;
import com.rubicon.data.thrift.types.TIntegerList;

import junit.framework.TestCase;

public class ThriftRawComparatorTestCase extends TestCase {

	public void testTIntegerListComparator() throws Exception {
		TIntegerList list1 = new TIntegerList(Arrays.asList(10, 12, 14));
		TIntegerList list2 = new TIntegerList(Arrays.asList(10, 12, 14));
		ThriftRawComparator<TIntegerList> comparator = new ThriftRawComparator<TIntegerList>();
		assertTrue(comparator.compare(list1, list2) == 0);
		SerializedResult ser = serialize(TIntegerList.class, list1, list2);
		assertEquals(comparator.compare(ser.bytes, ser.s1, ser.l1, ser.bytes,
				ser.s2, ser.l2), 0);

		// list1 > list2
		list1 = new TIntegerList(Arrays.asList(10, 12, 15));
		assertTrue(comparator.compare(list1, list2) == 1);
		ser = serialize(TIntegerList.class, list1, list2);
		assertEquals(comparator.compare(ser.bytes, ser.s1, ser.l1, ser.bytes,
				ser.s2, ser.l2), 1);

		// list1 < list2
		list1 = new TIntegerList(Arrays.asList(10, 12, 13));
		assertTrue(comparator.compare(list1, list2) == -1);
		ser = serialize(TIntegerList.class, list1, list2);
		assertEquals(comparator.compare(ser.bytes, ser.s1, ser.l1, ser.bytes,
				ser.s2, ser.l2), -1);
	}

	public void testUserProfileComparator() throws Exception {
		UserProfile user1 = new UserProfile(12, "hello", "world1");
		UserProfile user2 = new UserProfile(12, "hello", "world1");

		// should be equals
		ThriftRawComparator<UserProfile> comparator = new ThriftRawComparator<UserProfile>();
		assertTrue(comparator.compare(user1, user2) == 0);
		SerializedResult ser = serialize(UserProfile.class, user1, user2);
		assertEquals(comparator.compare(ser.bytes, ser.s1, ser.l1, ser.bytes,
				ser.s2, ser.l2), 0);

		// should be less than
		user1.setBlurb("world2");
		assertFalse(comparator.compare(user1, user2) == 0);
		ser = serialize(UserProfile.class, user1, user2);
		assertFalse(comparator.compare(ser.bytes, ser.s1, ser.l1, ser.bytes,
				ser.s2, ser.l2) == 0);

		// still not equal
		user2.setBlurb("world0");
		assertFalse(comparator.compare(user1, user2) == 0);
		ser = serialize(UserProfile.class, user1, user2);
		assertFalse(comparator.compare(ser.bytes, ser.s1, ser.l1, ser.bytes,
				ser.s2, ser.l2) == 0);

		// change length
		user1.setBlurb("world very long");
		assertFalse(comparator.compare(user1, user2) == 0);
		ser = serialize(UserProfile.class, user1, user2);
		assertFalse(comparator.compare(ser.bytes, ser.s1, ser.l1, ser.bytes,
				ser.s2, ser.l2) == 0);
	}

	private <T extends TBase> SerializedResult serialize(Class cls, T obj1,
			T obj2) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ThriftCompactSerializer<T> serializer = new ThriftCompactSerializer<T>();
		serializer.open(out);
		int s1 = out.size();
		serializer.serialize(obj1);
		int l1 = out.size();
		int s2 = out.size();
		serializer.serialize(obj2);
		int l2 = out.size() - s2;
		serializer.close();
		out.close();

		return new SerializedResult(out.toByteArray(), s1, l1, s2, l2);
	}

	private static class SerializedResult {
		public byte[] bytes;

		public int s1;

		public int l1;

		public int s2;

		public int l2;

		public SerializedResult(byte[] bytes, int s1, int l1, int s2, int l2) {
			this.bytes = bytes;
			this.s1 = s1;
			this.l1 = l1;
			this.s2 = s2;
			this.l2 = l2;
		}
	}

}

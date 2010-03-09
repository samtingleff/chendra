package com.rubicon.data.thrift.test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.rubicon.data.thrift.ThriftDeserializer;
import com.rubicon.data.thrift.ThriftRawComparator;
import com.rubicon.data.thrift.ThriftSerializer;
import com.rubicon.data.thrift.UserId;
import com.rubicon.data.thrift.UserProfile;

import junit.framework.TestCase;

public class ThriftSerializationTestCase extends TestCase {

	public void testUserIdSerialization() throws Exception {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		UserId uid1 = new UserId(12);
		ThriftSerializer<UserId> serializer = new ThriftSerializer<UserId>(
				UserId.class);
		serializer.open(out);
		serializer.serialize(uid1);
		serializer.close();

		byte[] bytes = out.toByteArray();
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);

		UserId uid2 = new UserId();
		ThriftDeserializer<UserId> deserializer = new ThriftDeserializer<UserId>(
				UserId.class);
		deserializer.open(in);
		deserializer.deserialize(uid2);
		deserializer.close();

		assertEquals(uid1, uid2);
		assertEquals(uid2.getUid(), 12);
	}

	public void testUserProfileSerialization() throws Exception {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		UserProfile user1 = new UserProfile(12, "hello", "world");
		ThriftSerializer<UserProfile> serializer = new ThriftSerializer<UserProfile>(
				UserProfile.class);
		serializer.open(out);
		serializer.serialize(user1);
		serializer.close();

		byte[] bytes = out.toByteArray();
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);

		UserProfile user2 = new UserProfile();
		ThriftDeserializer<UserProfile> deserializer = new ThriftDeserializer<UserProfile>(
				UserProfile.class);
		deserializer.open(in);
		deserializer.deserialize(user2);
		deserializer.close();

		assertEquals(user1, user2);
		assertEquals(user2.getUid(), 12);
		assertEquals(user2.getName(), "hello");
		assertEquals(user2.getBlurb(), "world");
	}

	public void testUserProfileComparator() throws Exception {
		UserProfile user1 = new UserProfile(12, "hello", "world1");
		UserProfile user2 = new UserProfile(12, "hello", "world1");

		// should be equals
		ThriftRawComparator<UserProfile> comparator = new ThriftRawComparator<UserProfile>();
		assertTrue(comparator.compare(user1, user2) == 0);
		SerializedResult ser = serialize(user1, user2);
		assertEquals(comparator.compare(ser.bytes, ser.s1, ser.l1, ser.bytes,
				ser.s2, ser.l2), 0);

		// should be less than
		user1.setBlurb("world2");
		assertFalse(comparator.compare(user1, user2) == 0);
		ser = serialize(user1, user2);
		assertFalse(comparator.compare(ser.bytes, ser.s1, ser.l1, ser.bytes,
				ser.s2, ser.l2) == 0);

		// still not equal
		user2.setBlurb("world0");
		assertFalse(comparator.compare(user1, user2) == 0);
		ser = serialize(user1, user2);
		assertFalse(comparator.compare(ser.bytes, ser.s1, ser.l1, ser.bytes,
				ser.s2, ser.l2) == 0);

		// change length
		user1.setBlurb("world very long");
		assertFalse(comparator.compare(user1, user2) == 0);
		ser = serialize(user1, user2);
		assertFalse(comparator.compare(ser.bytes, ser.s1, ser.l1, ser.bytes,
				ser.s2, ser.l2) == 0);
	}

	private SerializedResult serialize(UserProfile user1, UserProfile user2)
			throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ThriftSerializer<UserProfile> serializer = new ThriftSerializer<UserProfile>(
				UserProfile.class);
		serializer.open(out);
		int s1 = out.size();
		serializer.serialize(user1);
		int l1 = out.size();
		int s2 = out.size();
		serializer.serialize(user2);
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

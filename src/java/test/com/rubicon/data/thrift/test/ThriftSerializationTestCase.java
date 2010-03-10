package com.rubicon.data.thrift.test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import com.rubicon.data.thrift.ThriftCompactDeserializer;
import com.rubicon.data.thrift.ThriftCompactSerializer;
import com.rubicon.data.thrift.UserId;
import com.rubicon.data.thrift.UserProfile;

import junit.framework.TestCase;

public class ThriftSerializationTestCase extends TestCase {

	public void testUserIdSerialization() throws Exception {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		UserId uid1 = new UserId(12);
		ThriftCompactSerializer<UserId> serializer = new ThriftCompactSerializer<UserId>(
				UserId.class);
		serializer.open(out);
		serializer.serialize(uid1);
		serializer.close();

		byte[] bytes = out.toByteArray();
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);

		UserId uid2 = new UserId();
		ThriftCompactDeserializer<UserId> deserializer = new ThriftCompactDeserializer<UserId>(
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
		ThriftCompactSerializer<UserProfile> serializer = new ThriftCompactSerializer<UserProfile>(
				UserProfile.class);
		serializer.open(out);
		serializer.serialize(user1);
		serializer.close();

		byte[] bytes = out.toByteArray();
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);

		UserProfile user2 = new UserProfile();
		ThriftCompactDeserializer<UserProfile> deserializer = new ThriftCompactDeserializer<UserProfile>(
				UserProfile.class);
		deserializer.open(in);
		deserializer.deserialize(user2);
		deserializer.close();

		assertEquals(user1, user2);
		assertEquals(user2.getUid(), 12);
		assertEquals(user2.getName(), "hello");
		assertEquals(user2.getBlurb(), "world");
	}
}

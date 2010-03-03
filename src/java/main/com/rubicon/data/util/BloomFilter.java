package com.rubicon.data.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.BitSet;
import java.util.Collection;

import org.apache.hadoop.io.Writable;

import com.rubicon.data.util.io.ObjectSerializer;


/**
 * From http://blog.locut.us/2008/01/12/a-decent-stand-alone-java-bloom-filter-
 * implementation/
 * 
 * Changed to: 1) remove the Set interface; 2) use the HashFunction abstraction
 * rather than java.util.Random to build k hashes; 3) add() returns a boolean
 * value to indicate whether or not the item was already present.
 * 
 * A simple Bloom Filter (see http://en.wikipedia.org/wiki/Bloom_filter)
 * 
 * This code may be used, modified, and redistributed provided that the author
 * tag below remains intact.
 * 
 * @author Ian Clarke <ian@uprizer.com>
 * @author Sam Tingleff <sam@rubiconproject.com>
 * 
 * @param <E>
 *            The type of object the BloomFilter should contain
 */
public class BloomFilter<E extends Serializable> implements Serializable, Writable {
	private static final long serialVersionUID = 3527833617516722215L;

	protected HashFunction<E> hashFunction;

	protected int k;

	protected BitSet bitSet;

	protected int bitArraySize;
	
	protected int expectedElements;

	protected long count = 0;

	/**
	 * Provided for serialization/deserialization only. Do not use.
	 */
	public BloomFilter() {
	}

	/**
	 * Create a new bloom filter given an expected element count and desired
	 * false positive rate.
	 * 
	 * @param expectedElements
	 * @param desiredFalsePositiveRate
	 */
	public BloomFilter(int expectedElements, double desiredFalsePositiveRate) {
		this((int) Math.ceil((expectedElements * Math
				.log(desiredFalsePositiveRate))
				/ Math.log(1.0 / Math.pow(2, Math.log(2)))), expectedElements);
	}

	public BloomFilter(HashFunction<E> hashFunction, int expectedElements,
			double desiredFalsePositiveRate) {
		this(hashFunction, (int) Math.ceil((expectedElements * Math
				.log(desiredFalsePositiveRate))
				/ Math.log(1.0 / Math.pow(2, Math.log(2)))), expectedElements);
	}

	/**
	 * Create a new bloom filter with the given bit array size and expected
	 * element count.
	 * 
	 * @param bitArraySize
	 * @param expectedElements
	 */
	public BloomFilter(int bitArraySize, int expectedElements) {
		this(new SHA1HashFunction<E>(), bitArraySize, expectedElements);
	}

	/**
	 * Construct a SimpleBloomFilter. You must specify the number of bits in the
	 * Bloom Filter, and also you should specify the number of items you expect
	 * to add. The latter is used to choose some optimal internal values to
	 * minimize the false-positive rate (which can be estimated with
	 * expectedFalsePositiveRate()).
	 * 
	 * @param bitArraySize
	 *            The number of bits in the bit array (often called 'm' in the
	 *            context of bloom filters).
	 * @param expectedElements
	 *            The typical number of items you expect to be added to the
	 *            SimpleBloomFilter (often called 'n').
	 */
	public BloomFilter(HashFunction<E> hashFunction, int bitArraySize,
			int expectedElements) {
		this.hashFunction = hashFunction;
		this.bitArraySize = bitArraySize;
		this.expectedElements = expectedElements;
		this.k = (int) Math.ceil((bitArraySize / expectedElements)
				* Math.log(2.0));
		bitSet = new BitSet(bitArraySize);
	}

	/**
	 * Calculates the approximate probability of the contains() method returning
	 * true for an object that had not previously been inserted into the bloom
	 * filter. This is known as the "false positive probability".
	 * 
	 * @return The estimated false positive rate
	 */
	public double expectedFalsePositiveProbability() {
		return Math.pow((1 - Math.exp(-k * (double) expectedElements
				/ (double) bitArraySize)), k);
	}

	/**
	 * Add an element, returning true if the element was NOT already present.
	 * 
	 * @param o
	 * @return
	 */
	public boolean add(E o) {
		int found = 0;
		for (int x = 0; x < k; x++) {
			int h = (int) (hash(o, x) % bitArraySize);
			found += (bitSet.get(h)) ? 1 : 0;
			bitSet.set(h, true);
		}
		if (found < k)
			++count;
		return (found < k);
	}

	/**
	 * @return This method will always return false
	 */
	public boolean addAll(Collection<? extends E> c) {
		for (E o : c) {
			add(o);
		}
		return false;
	}

	/**
	 * Clear the Bloom Filter
	 */
	public void clear() {
		for (int x = 0; x < bitSet.length(); x++) {
			bitSet.set(x, false);
		}
		count = 0;
	}

	/**
	 * @return False indicates that o was definitely not added to this Bloom
	 *         Filter, true indicates that it probably was. The probability can
	 *         be estimated using the expectedFalsePositiveProbability() method.
	 */
	public boolean contains(E o) {
		for (int x = 0; x < k; x++) {
			int h = (int) (hash(o, x) % bitArraySize);
			if (!bitSet.get(h))
				return false;
		}
		return true;
	}

	public boolean containsAll(Collection<E> c) {
		for (E o : c) {
			if (!contains(o))
				return false;
		}
		return true;
	}

	public long getCount() {
		return count;
	}

	private long hash(E object, int rep) {
		return Math.abs(hashFunction.hash(object, rep));
	}

	public void readFields(DataInput in) throws IOException {
		this.k = in.readInt();
		this.bitArraySize = in.readInt();
		this.expectedElements = in.readInt();
		this.count = in.readLong();

		int hashFunctionByteLength = in.readInt();
		byte[] hashFunctionBytes = new byte[hashFunctionByteLength];
		in.readFully(hashFunctionBytes, 0, hashFunctionByteLength);
		this.hashFunction = ObjectSerializer.deserialize(hashFunctionBytes);

		int bitSetByteLength = in.readInt();
		byte[] bitSetBytes = new byte[bitSetByteLength];
		in.readFully(bitSetBytes, 0, bitSetByteLength);
		this.bitSet = (BitSet) ObjectSerializer.deserialize(bitSetBytes);
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(k);
		out.writeInt(bitArraySize);
		out.writeInt(expectedElements);
		out.writeLong(count);
		byte[] hashFunctionBytes = ObjectSerializer.serialize(hashFunction);
		out.writeInt(hashFunctionBytes.length);
		out.write(hashFunctionBytes);
		byte[] bitSetBytes = ObjectSerializer.serialize(bitSet);
		out.writeInt(bitSetBytes.length);
		out.write(bitSetBytes);
	}
}

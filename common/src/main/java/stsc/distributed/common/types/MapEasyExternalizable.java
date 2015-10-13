package stsc.distributed.common.types;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Common {@link Writable} map of fields. Could be used for storing any data
 * type with String / longs / integers / booleans / doubles field types (or
 * integral).
 */
public abstract class MapEasyExternalizable implements Externalizable {

	protected final Map<String, String> strings;
	protected final Map<String, Long> longs;
	protected final Map<String, Integer> integers;
	protected final Map<String, Boolean> booleans;
	protected final Map<String, Double> doubles;

	protected MapEasyExternalizable() {
		this.strings = new HashMap<>();
		this.longs = new HashMap<>();
		this.integers = new HashMap<>();
		this.booleans = new HashMap<>();
		this.doubles = new HashMap<>();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		writeStrings(out);
		writeLongs(out);
		writeIntegers(out);
		writeBooleans(out);
		writeDoubles(out);
	}

	private void writeStrings(ObjectOutput out) throws IOException {
		out.writeLong(strings.size());
		for (Entry<String, String> s : strings.entrySet()) {
			out.writeUTF(s.getKey());
			out.writeUTF(s.getValue());
		}
	}

	private void writeLongs(ObjectOutput out) throws IOException {
		out.writeLong(longs.size());
		for (Entry<String, Long> s : longs.entrySet()) {
			out.writeUTF(s.getKey());
			out.writeLong(s.getValue());
		}
	}

	private void writeIntegers(ObjectOutput out) throws IOException {
		out.writeLong(integers.size());
		for (Entry<String, Integer> s : integers.entrySet()) {
			out.writeUTF(s.getKey());
			out.writeInt(s.getValue());
		}
	}

	private void writeBooleans(ObjectOutput out) throws IOException {
		out.writeLong(booleans.size());
		for (Entry<String, Boolean> s : booleans.entrySet()) {
			out.writeUTF(s.getKey());
			out.writeBoolean(s.getValue());
		}
	}

	private void writeDoubles(ObjectOutput out) throws IOException {
		out.writeLong(doubles.size());
		for (Entry<String, Double> s : doubles.entrySet()) {
			out.writeUTF(s.getKey());
			out.writeDouble(s.getValue());
		}
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		readStrings(in);
		readLongs(in);
		readIntegers(in);
		readBooleans(in);
		readDoubles(in);
	}

	private void readStrings(ObjectInput in) throws IOException {
		final long sizeOfCollection = in.readLong();
		for (long i = 0; i < sizeOfCollection; ++i) {
			final String key = in.readUTF();
			final String value = in.readUTF();
			strings.put(key, value);
		}
	}

	private void readLongs(ObjectInput in) throws IOException {
		final long sizeOfCollection = in.readLong();
		for (long i = 0; i < sizeOfCollection; ++i) {
			final String key = in.readUTF();
			final Long value = in.readLong();
			longs.put(key, value);
		}
	}

	private void readIntegers(ObjectInput in) throws IOException {
		final long sizeOfCollection = in.readLong();
		for (long i = 0; i < sizeOfCollection; ++i) {
			final String key = in.readUTF();
			final Integer value = in.readInt();
			integers.put(key, value);
		}
	}

	private void readBooleans(ObjectInput in) throws IOException {
		final long sizeOfCollection = in.readLong();
		for (long i = 0; i < sizeOfCollection; ++i) {
			final String key = in.readUTF();
			final Boolean value = in.readBoolean();
			booleans.put(key, value);
		}
	}

	private void readDoubles(ObjectInput in) throws IOException {
		final long sizeOfCollection = in.readLong();
		for (long i = 0; i < sizeOfCollection; ++i) {
			final String key = in.readUTF();
			final Double value = in.readDouble();
			doubles.put(key, value);
		}
	}

}

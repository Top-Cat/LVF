package uk.co.thomasc.lvf.packets;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;

import lombok.Getter;

public enum Packets {
	Packet0Kill(0, Packet0Kill.class),
	Packet1Withdraw(1, Packet1Withdraw.class, true),
	Packet2Delete(2, Packet2Delete.class, true),
	Packet3Merge(3, Packet3Merge.class, true),
	Packet4Result(4, Packet4Result.class, true),
	;
	
	@Getter private int id;
	private Class<? extends Packet> clazz;
	@Getter private boolean info;
	
	private Packets(int id, Class<? extends Packet> clazz) {
		this(id, clazz, false);
	}
	
	private Packets(int id, Class<? extends Packet> clazz, boolean info) {
		this.id = id;
		this.clazz = clazz;
		this.info = info;
	}
	
	public Packet createInstance() {
		try {
			final Constructor<? extends Packet> c = clazz.getDeclaredConstructor(new Class[] {});
			return c.newInstance();
		} catch (final NoSuchMethodException e) {
			e.printStackTrace();
		} catch (final SecurityException e) {
			e.printStackTrace();
		} catch (final InstantiationException e) {
			e.printStackTrace();
		} catch (final IllegalAccessException e) {
			e.printStackTrace();
		} catch (final IllegalArgumentException e) {
			e.printStackTrace();
		} catch (final InvocationTargetException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	private static HashMap<Integer, Packets> map = new HashMap<Integer, Packets>();

	public static Packets getPacket(int id) {
		return map.get(id);
	}

	static {
		for (final Packets pks : Packets.values()) {
			map.put(pks.getId(), pks);
		}
	}
}
package uk.co.thomasc.lvf.packets;

import uk.co.thomasc.lvf.network.BinaryReader;

public interface Packet {
	
	public void readPacket(BinaryReader input) throws Exception;
	
	public byte[] getBytes() throws Exception;
	
	public int getId();
	
	public int getVersion();
	
}
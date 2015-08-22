package uk.co.thomasc.lvf.packets;

import java.io.IOException;

import uk.co.thomasc.lvf.network.BinaryReader;
import lombok.Getter;
import lombok.Setter;

public abstract class TaskPacket implements Packet {

	@Getter @Setter private boolean success = false;
	
	public TaskPacket() {
		
	}
	
	public void readPacket(BinaryReader input) throws IOException {
		
	}

	public byte[] getBytes() throws IOException {
		return new byte[0];
	}
	
	public int getVersion() {
		return 0;
	}
	
}
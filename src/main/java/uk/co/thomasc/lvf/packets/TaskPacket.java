package uk.co.thomasc.lvf.packets;

import java.io.IOException;

import uk.co.thomasc.lvf.network.BinaryReader;
import uk.co.thomasc.lvf.network.BinaryWriter;
import lombok.Getter;
import lombok.Setter;

public abstract class TaskPacket implements Packet {

	@Getter @Setter private int uvi;
	@Getter @Setter private boolean success = false;
	
	public TaskPacket() {
		
	}
	
	public void readPacket(BinaryReader input) throws IOException {
		setUvi(input.readInt());
	}

	public byte[] getBytes() throws IOException {
		BinaryWriter bw = new BinaryWriter();
		
		bw.write(getUvi());
		
		return bw.toByteArray();
	}
	
	public int getVersion() {
		return 0;
	}
	
}
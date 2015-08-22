package uk.co.thomasc.lvf.packets;

import java.io.IOException;

import uk.co.thomasc.lvf.network.BinaryReader;
import uk.co.thomasc.lvf.network.BinaryWriter;
import lombok.Getter;
import lombok.Setter;

public abstract class BusPacket extends TaskPacket {

	@Getter @Setter private int uvi;
	
	public BusPacket() {
		
	}
	
	public void readPacket(BinaryReader input) throws IOException {
		super.readPacket(input);
		setUvi(input.readInt());
	}

	public byte[] getBytes() throws IOException {
		BinaryWriter bw = new BinaryWriter();
		
		bw.write(super.getBytes());
		bw.write(getUvi());
		
		return bw.toByteArray();
	}
	
	public int getVersion() {
		return super.getVersion() + 0;
	}
	
}
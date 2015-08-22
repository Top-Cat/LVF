package uk.co.thomasc.lvf.network;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import lombok.Getter;
import uk.co.thomasc.lvf.packets.Packet;
import uk.co.thomasc.lvf.packets.Packet0Kill;
import uk.co.thomasc.lvf.packets.Packet1Result;
import uk.co.thomasc.lvf.packets.TaskPacket;

public class TaskServer extends Thread {

	@Getter private SynchronousQueue<TaskPacket> taskQueue = new SynchronousQueue<TaskPacket>();
	
	private ServerSocket serverSocket;
	private Socket socket;
	private boolean running = true;
	
	public TaskServer() {
		start();
	}
	
	public void run() {
		while (running) {
			try {
				serverSocket = new ServerSocket(5350, 100);
				while (true) {
					try {
						socket = serverSocket.accept();
						
						while (true) {
							Packet packet = Network.listenForPacket(socket);
							if (packet instanceof Packet0Kill) {
								break;
							} else if (packet instanceof TaskPacket) {
								synchronized (packet) {
									if (taskQueue.offer((TaskPacket) packet, 1, TimeUnit.SECONDS)) {
										packet.wait();
										
										Packet1Result result = new Packet1Result();
										result.setSuccess(((TaskPacket) packet).isSuccess());
										Network.sendPacket(socket, result);
									} else {
										// We've failed the user, abort
										break;
									}
								}
							}
						}
					} catch (IOException e) {
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						try {
							socket.close();
						} catch (IOException e) {};
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public void finish() {
		try {
			running = false;
			serverSocket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
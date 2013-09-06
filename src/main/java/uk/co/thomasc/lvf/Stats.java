package uk.co.thomasc.lvf;

public class Stats extends Thread {
	
	private boolean running = true;
	private int rows = 0;
	
	public Stats() {
		start();
	}
	
	public void incRows() {
		rows++;
	}
	
	@Override
	public void run() {
		while (running) {
			try {
				sleep(1000);
				
				System.out.println("Rows: " + rows + "\r");
				
				rows = 0;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public void finish() {
		running = false;
	}
	
}
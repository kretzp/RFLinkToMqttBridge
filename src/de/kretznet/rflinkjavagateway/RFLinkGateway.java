package de.kretznet.rflinkjavagateway;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import gnu.io.CommPort;
import gnu.io.CommPortIdentifier;
import gnu.io.SerialPort;
import gnu.io.SerialPortEvent;
import gnu.io.SerialPortEventListener;

public class RFLinkGateway {

	public RFLinkGateway() {
		super();
	}

	static OutputStream out;
	static AsyncCallback cb;
	static String topic = "/data/RFLINK";
	static String mqttserver = "tcp://192.168.39.104:1883";
	static String user = "openhabian";
	static String password = "letsdoit";
	
	public static void log(String string) {
		String timestamp = ZonedDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss:SSS"));
		System.out.println(timestamp + " " + string);
	}

	void write(String toWrite) throws IOException {
		out.write(toWrite.getBytes());
		out.write('\r');
		out.write('\n');
		log("Sent to com: " + toWrite);
	}

	void connect(String portName) throws Exception {
		log("Open port: " + portName);
		CommPortIdentifier portIdentifier = CommPortIdentifier.getPortIdentifier(portName);
		if (portIdentifier.isCurrentlyOwned()) {
			log("Error: Port is currently in use");
		} else {
			CommPort commPort = portIdentifier.open(this.getClass().getName(), 2000);

			if (commPort instanceof SerialPort) {
				SerialPort serialPort = (SerialPort) commPort;
				serialPort.setSerialPortParams(57600, SerialPort.DATABITS_8, SerialPort.STOPBITS_1,
						SerialPort.PARITY_NONE);

				InputStream in = serialPort.getInputStream();
				out = serialPort.getOutputStream();

				serialPort.addEventListener(new SerialReader(in));
				serialPort.notifyOnDataAvailable(true);

			} else {
				log("Error: Only serial ports are handled by this example.");
			}
		}
	}

	static String getLeftSideFromEqual(String in) {
		int index = in.indexOf('=');
		if (index > -1) {
			return in.substring(index + 1);
		}

		return in;
	}

	public static String[] convertSerialToMqtt(String in) {
		String strings[] = in.split(";");

		StringBuffer outString = new StringBuffer(topic);
		for (int i = 2; i < strings.length - 3; i++) {
			outString.append("/").append(getLeftSideFromEqual(strings[i]));
		}

		outString.append("/").append("R").append("/").append(getLeftSideFromEqual(strings[strings.length - 3]));

		String[] retStrings = new String[2];

		retStrings[0] = outString.substring(0);
		retStrings[1] = getLeftSideFromEqual(strings[strings.length - 2]);

		return retStrings;
	}

	/**
	 * 20;5F;Dooya;ID=7d3412ff;SWITCH=ff;CMD=DOWN; --
	 * /data/RFLINK/Dooya/7d3412ff/R/ff DOWN
	 * 
	 * @param in
	 * @param payload
	 * @return
	 * @throws IllegalArgumentException
	 */
	public static String convertMqttToSerial(String in, String payload) throws IllegalArgumentException {
		String strings[] = in.split("/");

		if (!strings[0].equals(topic.split("/")[0])) {
			throw new IllegalArgumentException("Error, topic malform on position 0!");

		}
		if (!strings[1].equals(topic.split("/")[1])) {
			throw new IllegalArgumentException("Error, topic malform on position 1!");
		}
		if (!strings[strings.length - 2].equals("W")) {
			throw new IllegalArgumentException("Error, topic malform on position length - 2, no W!",
					new Throwable("W"));
		}

		StringBuffer outString = new StringBuffer("10");
		for (int i = 3; i < strings.length - 2; i++) {
			outString.append(";").append(strings[i]);
		}

		outString.append(";").append(strings[strings.length - 1]).append(";").append(payload).append(";");

		return outString.substring(0);
	}

	/**
	 * Handles the input coming from the serial port. A new line character is
	 * treated as the end of a block in this example.
	 */
	public static class SerialReader implements SerialPortEventListener {
		private InputStream in;
		private byte[] buffer = new byte[1024];

		public SerialReader(InputStream in) {
			this.in = in;
		}

		public void serialEvent(SerialPortEvent arg0) {
			int data;

			try {
				int len = 0;
				while ((data = in.read()) > -1) {
					if (data == '\n') {
						break;
					}
					buffer[len++] = (byte) data;
				}
				String cominput = new String(buffer, 0, len);
				log("Read from com port: " + cominput);
				String[] cominputs = convertSerialToMqtt(cominput);
				cb.publish(cominputs[0], 2, cominputs[1].getBytes());
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}

	}

	/**
	 * Arg 0: comport
	 * Arg 1: topic
	 * Arg 2: server
	 * Arg 3: user
	 * Arg 4: password
	 * 
	 * @param args
	 * @throws InterruptedException
	 * @throws UnknownHostException 
	 */
	public static void main(String[] args) throws InterruptedException, UnknownHostException {
		RFLinkGateway com = new RFLinkGateway();
		String comport = "COM6";
		if (args.length > 0) {
			comport = args[0];
		}
		if(args.length > 1) {
			topic = args[1];
		}
		if(args.length > 2) {
			mqttserver = args[2];
		}
		if(args.length > 3) {
			user = args[3];
		}
		if(args.length > 4) {
			password = args[4];
		}

		try {
			com.connect(comport);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		cb = new AsyncCallback(mqttserver, InetAddress.getLocalHost().getHostName() + "-RFLINK", true, false, "openhabian", "letsdoit", out);
		cb.subscribe(topic + "/#", 2);

		int counter = 0;
		
		cb.publish(topic + "/R/systime", 0, ZonedDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm")).getBytes());
		
		while (true) {
			counter++;
			Thread.sleep(5000);
			if(counter == 12) {
				counter = 0;
				cb.publish(topic + "/R/systime", 0, ZonedDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm")).getBytes());
			}
			cb.connect();
		}
	}

}

package de.kretznet.rflinkjavagateway;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Timestamp;
import java.util.Arrays;

import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;

public class AsyncCallback implements MqttCallbackExtended {

	// Private instance variables
	MqttAsyncClient client;
	String brokerUrl;
	private boolean quietMode;
	private MqttConnectOptions conOpt;
	private boolean clean;
	Object waiter = new Object();
	private String password;
	private String userName;
	private boolean isConnectingMqtt = false;
	static OutputStream out;

	/**
	 * Constructs an instance of the sample client wrapper
	 * 
	 * @param brokerUrl    the url to connect to
	 * @param clientId     the client id to connect with
	 * @param cleanSession clear state at end of connection or not (durable or
	 *                     non-durable subscriptions)
	 * @param quietMode    whether debug should be printed to standard out
	 * @param userName     the username to connect with
	 * @param password     the password for the user
	 * @throws MqttException
	 */
	public AsyncCallback(String brokerUrl, String clientId, boolean cleanSession, boolean quietMode, String userName,
			String password, OutputStream out) {
		this.brokerUrl = brokerUrl;
		this.quietMode = quietMode;
		this.clean = cleanSession;
		this.password = password;
		this.userName = userName;
		AsyncCallback.out = out;
		// This sample stores in a temporary directory... where messages temporarily
		// stored until the message has been delivered to the server.
		// ..a real application ought to store them somewhere
		// where they are not likely to get deleted or tampered with
		String tmpDir = System.getProperty("java.io.tmpdir");
		MqttDefaultFilePersistence dataStore = new MqttDefaultFilePersistence(tmpDir);

		try {
			// Construct the object that contains connection parameters
			// such as cleanSession and LWT
			conOpt = new MqttConnectOptions();
			conOpt.setCleanSession(clean);
			if (password != null) {
				conOpt.setPassword(this.password.toCharArray());
			}
			if (userName != null) {
				conOpt.setUserName(this.userName);
			}

			// Construct the MqttClient instance
			client = new MqttAsyncClient(this.brokerUrl, clientId, dataStore);

			// Set this wrapper as the callback handler
			client.setCallback(this);

		} catch (MqttException e) {
			e.printStackTrace();
			log("Unable to set up client: " + e.toString());
			System.exit(1);
		}
	}

	/**
	 * @see MqttCallback#connectionLost(Throwable)
	 */
	public void connectionLost(Throwable cause) {
		// Called when the connection to the server has been lost.
		// An application may choose to implement reconnection
		// logic at this point. This sample simply exits.
		log("Connection to " + brokerUrl + " lost! " + cause);
		log(cause.fillInStackTrace().toString());
		// connect();
	}

	/**
	 * @see MqttCallback#messageArrived(String, MqttMessage)
	 */
	public void messageArrived(String topic, MqttMessage message) throws MqttException {
		// Called when a message arrives from the server that matches any
		// subscription made by the client
		String payload = new String(message.getPayload());
		log("Topic:\t" + topic + "  Message:\t" + payload + "  QoS:\t" + message.getQos());
		try {
			write(RFLinkGateway.convertMqttToSerial(topic, payload));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException iae) {
			if (iae.getCause().getMessage().equals("W")) {
				log("Read message, nothing to do");
			} else {
				// TODO Auto-generated catch block
				iae.printStackTrace();
			}
		}
	}

	void write(String toWrite) throws IOException {
		out.write(toWrite.getBytes());
		out.write('\r');
		out.write('\n');
		log("Wrote to com port: " + toWrite);
	}

	/**
	 * @see MqttCallback#deliveryComplete(IMqttDeliveryToken)
	 */
	public void deliveryComplete(IMqttDeliveryToken token) {
		// Called when a message has been delivered to the
		// server. The token passed in here is the same one
		// that was returned from the original call to publish.
		// This allows applications to perform asynchronous
		// delivery without blocking until delivery completes.
		//
		// This sample demonstrates asynchronous deliver, registering
		// a callback to be notified on each call to publish.
		//
		// The deliveryComplete method will also be called if
		// the callback is set on the client
		//
		// note that token.getTopics() returns an array so we convert to a string
		// before printing it on the console
		log("Delivery complete callback: Publish Completed " + Arrays.toString(token.getTopics()));
	}

	@Override
	public void connectComplete(boolean reconnect, String serverURI) {
		isConnectingMqtt = false;
		log("Connection completed: " + serverURI + " reconnect: " + reconnect);
	}

	/**
	 * Utility method to handle logging. If 'quietMode' is set, this method does
	 * nothing
	 * 
	 * @param message the message to log
	 */
	public void log(String message) {
		if (!quietMode) {
			RFLinkGateway.log(message);
		}
	}

	public void connect() {
		// Connect using a blocking connect
		try {
			if(!isConnectingMqtt && !client.isConnected()) {
				log("CLient is not connected!");
				isConnectingMqtt = true;
				client.connect(conOpt);
				log("CLient should be connected!");
			}
		} catch (MqttException e) {
			isConnectingMqtt = false;
			log("connect failed: " + e);
		}
	}

	/**
	 * Publish / send a message to an MQTT server
	 * 
	 * @param topicName the name of the topic to publish to
	 * @param qos       the quality of service to delivery the message at (0,1,2)
	 * @param payload   the set of bytes to send to the MQTT server
	 * @throws MqttException
	 */
	public void publish(String topicName, int qos, byte[] payload) {
		// Publish using a non-blocking publisher
		Publisher pub = new Publisher();
		pub.doPublish(topicName, qos, payload);
	}

	/**
	 * Publish in a non-blocking way and then sit back and wait to be notified that
	 * the action has completed.
	 */
	public class Publisher {
		public void doPublish(String topicName, int qos, byte[] payload) {
			// Send / publish a message to the server
			// Get a token and setup an asynchronous listener on the token which
			// will be notified once the message has been delivered
			MqttMessage message = new MqttMessage(payload);
			message.setQos(qos);

			String time = new Timestamp(System.currentTimeMillis()).toString();
			log("Publishing at: " + time + " to topic \"" + topicName + "\" qos " + qos);

			// Setup a listener object to be notified when the publish completes.
			//
			IMqttActionListener pubListener = new IMqttActionListener() {
				public void onSuccess(IMqttToken asyncActionToken) {
					log("Publish Completed: " + time + " to topic \"" + topicName + "\" qos " + qos);
					carryOn();
				}

				public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
					log("Publishing failed: " + time + " to topic \"" + topicName + "\" qos " + qos);
					log("Publishing failed" + exception);
					carryOn();
				}

				public void carryOn() {
					synchronized (waiter) {
						waiter.notifyAll();
					}
				}
			};

			try {
				if (!client.isConnected()) {
					log("Client is not connected, try reconnect!");
					connect();
				}
				while (!client.isConnected()) {
				}
				client.publish(topicName, message, "Pub sample context", pubListener);
			} catch (MqttException e) {
				log("publish failed: " + e);
			}
		}
	}

	/**
	 * Subscribe to a topic on an MQTT server Once subscribed this method waits for
	 * the messages to arrive from the server that match the subscription. It
	 * continues listening for messages until the enter key is pressed.
	 * 
	 * @param topicName to subscribe to (can be wild carded)
	 * @param qos       the maximum quality of service to receive messages at for
	 *                  this subscription
	 * @throws MqttException
	 */
	public void subscribe(String topicName, int qos) {
		Subscriber sub = new Subscriber();
		sub.doSubscribe(topicName, qos);
	}

	/**
	 * Subscribe in a non-blocking way and then sit back and wait to be notified
	 * that the action has completed.
	 */
	public class Subscriber {
		public void doSubscribe(String topicName, int qos) {
			// Make a subscription
			// Get a token and setup an asynchronous listener on the token which
			// will be notified once the subscription is in place.
			log("Subscribing to topic \"" + topicName + "\" qos " + qos);

			IMqttActionListener subListener = new IMqttActionListener() {
				public void onSuccess(IMqttToken asyncActionToken) {
					log("Subscribe Completed");
					carryOn();
				}

				public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
					log("Subscribe failed" + exception);
					carryOn();
				}

				public void carryOn() {
					synchronized (waiter) {
						waiter.notifyAll();
					}
				}
			};

			try {

				if (!client.isConnected()) {
					log("Client is not connected, try reconnect!");
					connect();
				}
				while (!client.isConnected()) {
				}
				client.subscribe(topicName, qos, "Subscribe sample context", subListener);
			} catch (MqttException e) {
				log("subscribe failed: " + e);
			}
		}
	}

	/**
	 * Disconnect in a non-blocking way and then sit back and wait to be notified
	 * that the action has completed.
	 */
	public class Disconnector {
		public void doDisconnect() {
			// Disconnect the client
			log("Disconnecting");

			IMqttActionListener discListener = new IMqttActionListener() {
				public void onSuccess(IMqttToken asyncActionToken) {
					log("Disconnect Completed");
					carryOn();
				}

				public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
					log("Disconnect failed" + exception);
					carryOn();
				}

				public void carryOn() {
					synchronized (waiter) {
						waiter.notifyAll();
					}
				}
			};

			try {
				client.disconnect("Disconnect sample context", discListener);
				isConnectingMqtt = false;
			} catch (MqttException e) {
				isConnectingMqtt = false;
				log("disconnect failed: " + e);
			}
		}
	}
}

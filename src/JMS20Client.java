import javax.jms.*;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class JMS20Client {

    private String serverUrl = "tcp://localhost:7222";
    private String name = "topic.sample";
    private String messageType = "text";
    private String message = "Howdy JMS 2.0!";
    private int messageQty = 1;
    private boolean useTopic = true;
    private boolean useAsync = true;
    private boolean useAsyncReceive = true;
    private boolean badMessage = false;
    private boolean sharedConsumer = false;

    private JMSProducer msgProducer = null;
    private JMSConsumer msgConsumer = null;
    private JMSConsumer helperMsgConsumer = null;
    private Destination destination = null;
    private JMSContext context = null;

    private CountDownLatch latch = new CountDownLatch(1);
    private TotesCompletionListener totesCompletionListener = new TotesCompletionListener(latch);

    public static void main(String[] args) {
        JMS20Client client = new JMS20Client();
        client.run();
    }

    public void run(){
        setup();
        publish();
        if (!useAsyncReceive) {
            System.out.println("Message received: " + receive());
        }
    }

    void setup() {
        ConnectionFactory factory = new com.tibco.tibjms.TibjmsConnectionFactory(serverUrl);

        // Create JMSContext
        context = factory.createContext();
        context.setAutoStart(true);

        try {

            // Create Destination
            if (useTopic)
                destination = context.createTopic(name);
            else
                destination = context.createQueue(name);

            // Create JMSConsumer
            if (sharedConsumer) {
                msgConsumer = context.createSharedConsumer((Topic)destination, "totes");
                helperMsgConsumer = context.createSharedConsumer((Topic)destination, "totes");

                msgConsumer.setMessageListener(new MessageListener() {
                    @Override
                    public void onMessage(Message message) {
                        processMessage(message);
                    }
                });

                helperMsgConsumer.setMessageListener(new MessageListener() {
                    @Override
                    public void onMessage(Message message) {
                        System.out.println("Helper: I'm not yo help!");
                    }
                });
            } else {
                msgConsumer = context.createConsumer(destination);
                if (useAsyncReceive) {
                    msgConsumer.setMessageListener(new MessageListener() {
                        @Override
                        public void onMessage(Message message) {
                            processMessage(message);
                        }
                    });
                }
            }

            // Create JMSProducer
            if (useAsync)
                msgProducer = context.createProducer()
                                     .setAsync(totesCompletionListener)
                                     .setDeliveryDelay(200)
                                     .setDeliveryMode(DeliveryMode.PERSISTENT);
            else
                msgProducer = context.createProducer();

        } catch (JMSRuntimeException e) {
            System.err.println("EXCEPTION: " + e.getMessage());
        }
    }

    void publish() {
        while (messageQty != 0) {
            messageQty--;
            System.out.println("Publishing to destination: " + name);

            Message msg = createMessage();

            if (useAsync)
                msgProducer.send(destination, msg);
            else
                msgProducer.send(destination, msg);

            System.out.println("Message Published");
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if (totesCompletionListener.getException() == null){
            System.out.println("All replies received from the server");
        } else {
            System.err.println(totesCompletionListener.getException().getMessage());
        }
    }

    void processMessage(Message message) {
        System.out.println("Processing message...");

        try {
            if (messageType.toLowerCase().equals("text"))
                System.out.println("TextMessage received: " + ((TextMessage)message).getText());
            else if (messageType.toLowerCase().equals("bytes")) {
                javax.jms.BytesMessage bytesMessage = (javax.jms.BytesMessage) message;
                byte[] bytes = new byte[(int) bytesMessage.getBodyLength()];
                bytesMessage.readBytes(bytes);
                String bytesToStringMsg = new String(bytes, "UTF-8");
                System.out.println("BytesMessage received: " + bytesToStringMsg);
            }
            else if (messageType.toLowerCase().equals("map"))
                System.out.println("MapMessage received: " + ((MapMessage)message).getString("message"));
            else
                System.out.println("Received unrecognized message type");
        } catch (JMSException e) {
            System.err.println("EXCEPTION: " + e.getMessage());
        } catch (UnsupportedEncodingException e) {
            System.err.println("EXCEPTION: " + e.getMessage());
        }

        if (badMessage) {
            try {
                int deliveryCount = message.getIntProperty("JMSXDeliveryCount");
                if (deliveryCount<10){
                    // now throw a RuntimeException
                    // to simulate a problem processing the message
                    // the message will then be redelivered
                    throw new RuntimeException("Exception thrown to simulate a bad message");
                } else {
                    // message has been redelivered ten times,
                    // let's do something to prevent endless redeliveries
                    // such as sending it to dead message queue
                    System.out.println("Bad Message discarded!");
                }
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        }
    }

    Message createMessage() {
        try {
            if (messageType.toLowerCase().equals("text")) {
                return context.createTextMessage(message);
            }
            else if (messageType.toLowerCase().equals("bytes")) {
                BytesMessage bytesMessage = context.createBytesMessage();
                bytesMessage.writeBytes(message.getBytes());
                return bytesMessage;
            }
            else if (messageType.toLowerCase().equals("map")) {
                MapMessage mapMessage = context.createMapMessage();
                mapMessage.setString("message", message);
                return mapMessage;
            } else{
                throw new JMSException("Unsupported message type: " + messageType);
            }
        } catch (JMSException e) {
            System.err.println("EXCEPTION: " + e.getMessage());
        }
        return null;
    }

    public String receive(){
        if (messageType.toLowerCase().equals("text"))
            return msgConsumer.receiveBody(String.class);
        else if (messageType.toLowerCase().equals("bytes"))
            return msgConsumer.receiveBody(byte[].class).toString();
        else if (messageType.toLowerCase().equals("map"))
            return (String) msgConsumer.receiveBody(Map.class).get("message");
        else
            return null;
    }

    class TotesCompletionListener implements CompletionListener {
        CountDownLatch latch;
        Exception exception;

        public TotesCompletionListener(CountDownLatch latch) {
            this.latch=latch;
        }

        public void onCompletion(Message msg) {
            System.out.println("Successfully sent message");

            latch.countDown();
        }

        @Override
        public void onException(Message message, Exception exception) {
            System.err.println("Error sending message");
            latch.countDown();
            this.exception=exception;
        }

        public Exception getException(){
            return exception;
        }
    }
}

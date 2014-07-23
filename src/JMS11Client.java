import javax.jms.*;
import java.io.UnsupportedEncodingException;

public class JMS11Client implements ExceptionListener {

    private String serverUrl = "tcp://localhost:7222";
    private String name = "topic.sample";
    private String messageType = "map";
    private String message = "Hasta La Vista JMS 1.1!";
    private int messageQty = 1;
    private boolean useTopic = true;
    private boolean useAsyncReceive = true;

    private Connection connection = null;
    private Session session = null;
    private MessageProducer msgProducer = null;
    private MessageConsumer msgConsumer = null;
    private Destination destination = null;

    public static void main(String[] args) {
        JMS11Client client = new JMS11Client();
        client.run();
    }

    public void run(){
        try {
            setup();
            publish();
            if (!useAsyncReceive) {
                System.out.println("Message received: " + receive());
            }
        } finally {
            try {
                System.out.println("Closing connection...");
                if(connection != null)
                    connection.close();
            } catch (JMSException e) {
                System.err.println("EXCEPTION: " + e.getMessage());
            }
        }
    }

    @Override
    public void onException(JMSException e) {
        System.err.println("CONNECTION EXCEPTION: " + e.getMessage());
    }

    void setup() {
        try {
            ConnectionFactory factory = new com.tibco.tibjms.TibjmsConnectionFactory(serverUrl);
            connection = factory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            connection.setExceptionListener(this);

            if (useTopic)
                destination = session.createTopic(name);
            else
                destination = session.createQueue(name);

            msgProducer = session.createProducer(destination);
            msgConsumer = session.createConsumer(destination);

            if (useAsyncReceive) {
                msgConsumer.setMessageListener(new MessageListener() {
                    @Override
                    public void onMessage(Message message) {
                        processMessage(message);
                    }
                });
            }

            connection.start();

        } catch(JMSException e) {
            System.err.println("EXCEPTION: " + e.getMessage());
        }
    }

    void publish() {
        while (messageQty != 0) {
            messageQty--;
            try {
                System.out.println("Publishing to destination: " + name);

                Message msg = createMessage();
                msgProducer.send(msg);

                System.out.println("Message Published");
            }
            catch (JMSException e) {
                System.err.println("EXCEPTION: " + e.getMessage());
            }
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
    }

    Message createMessage() {
        try {
            if (messageType.toLowerCase().equals("text")) {
                return session.createTextMessage(message);
            }
            else if (messageType.toLowerCase().equals("bytes")) {
                BytesMessage bytesMessage = session.createBytesMessage();
                bytesMessage.writeBytes(message.getBytes());
                return bytesMessage;
            }
            else if (messageType.toLowerCase().equals("map")) {
                MapMessage mapMessage = session.createMapMessage();
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
        String body=null;
        try {
            if (messageType.toLowerCase().equals("text")) {
                TextMessage textMessage = (TextMessage)msgConsumer.receive();
                body = textMessage.getText();
            }
            else if (messageType.toLowerCase().equals("bytes")) {
                BytesMessage bytesMessage = (BytesMessage)msgConsumer.receive();
                body = bytesMessage.readUTF();
            }
            else if (messageType.toLowerCase().equals("map")) {
                MapMessage mapMessage = (MapMessage)msgConsumer.receive();
                body = mapMessage.getString("message");
            } else{
                throw new JMSException("Unsupported message type: " + messageType);
            }
        } catch (JMSException e) {
            System.err.println("EXCEPTION: " + e.getMessage());
        }
        return body;
    }
}

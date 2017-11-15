import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.concurrent.TimeoutException;

public class Send {

    private final static String QUEUE_NAME = "hello";
    private final static String HOST = "106.14.5.254";

    public static void main(String[] argv) throws java.io.IOException, TimeoutException {
        //create connection
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        factory.setPort(5672);
        factory.setUsername("kevin");
        factory.setPassword("123456");
        Connection connection = factory.newConnection();

        //create a channel
        Channel channel = connection.createChannel();
        //declare a message queue
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        String message = "Hello World!";
        //send msg
        channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
        System.out.println(" [x] Sent '" + message + "'");
        //last close connect
        channel.close();
        connection.close();

    }

}


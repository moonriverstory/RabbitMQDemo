package route;

import com.rabbitmq.client.*;

import java.io.IOException;

public class ReceiveLogsDirect {
    private static final String EXCHANGE_NAME = "direct_logs";
    private final static String HOST = "106.14.5.254";

    public static void main(String[] argv) throws Exception {
        //设置参数
        argv = new String[]{"warning"};

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        factory.setPort(5672);
        factory.setUsername("kevin");
        factory.setPassword("123456");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        String queueName = channel.queueDeclare().getQueue();

        if (argv.length < 1) {
            System.err.println("Usage: ReceiveLogsDirect [info] [warning] [error]");
            System.exit(1);
        }

        for (String severity : argv) {
            channel.queueBind(queueName, EXCHANGE_NAME, severity);
        }
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println(" [x] Received '" + envelope.getRoutingKey() + "':'" + message + "'");
            }
        };
        channel.basicConsume(queueName, true, consumer);
    }
}

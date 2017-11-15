package queue;

import com.rabbitmq.client.*;

import java.io.IOException;

public class Worker {
    private static final String TASK_QUEUE_NAME = "task_queue";
    private final static String HOST = "106.14.5.254";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        factory.setPort(5672);
        factory.setUsername("kevin");
        factory.setPassword("123456");
        final Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();

        channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
        //公平分发：每次只处理一个 =。=
        //但是，你要注意到：我在把autoAck设置为true的时候，运行worker，停掉服务器，没处理完的那几个消息也都在后续处理了，
        //而不是因为每次只处理一个，就没有都取出来=。= 这个设计还好吧~
        int prefetchCount = 1;
        channel.basicQos(1);

        final Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");

                System.out.println(" [x] Received '" + message + "'");
                try {
                    doWork(message);
                } finally {
                    System.out.println(" [x] Done");
                    //返回确认消息给mq
                    channel.basicAck(envelope.getDeliveryTag(), false);
                }
            }
        };
        //消息确认标志：true:收到消息即返回确认消息，false:处理完任务在finally里面手动的返回确认
        boolean autoAck = false;
        /*
        [*] Waiting for messages. To exit press CTRL+C
        [x] Received 'Hello World!.....'
         */
        //设置为true太恶心了，直接全部取出，根本不考虑线程的处理过程，能取多少取多少
        //这时，如果处理线程崩溃了，取出的数据全部消失！
        //可恶，怎么会允许这种大bug一类的参数配置可行呢！
        //是的，做了个实验，是都取出来了=。=
        //连接断开后，也继续处理已取出的数据！
        channel.basicConsume(TASK_QUEUE_NAME, autoAck, consumer);
    }

    private static void doWork(String task) {
        for (char ch : task.toCharArray()) {
            if (ch == '.') {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException _ignored) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

}

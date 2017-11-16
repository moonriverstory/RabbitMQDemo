package route;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class EmitLogDirect {
    private static final String EXCHANGE_NAME = "direct_logs";
    private final static String HOST = "106.14.5.254";

    public static void main(String[] argv) throws Exception {
        //设置参数
        argv = new String[]{"warning", "Run. Run. Or it will explode!"};

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        factory.setPort(5672);
        factory.setUsername("kevin");
        factory.setPassword("123456");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        //创建direct交换机
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

        String severity = getSeverity(argv);
        String message = getMessage(argv);

        //这里发布一条消息=。=
        channel.basicPublish(EXCHANGE_NAME, severity, null, message.getBytes("UTF-8"));
        System.out.println(" [x] Sent '" + severity + "':'" + message + "'");

        channel.close();
        connection.close();
    }

    private static String getSeverity(String[] strings){
        if (strings.length < 1)
            return "info";
        return strings[0];
    }

    private static String getMessage(String[] strings){
        if (strings.length < 2)
            return "Hello World!";
        return joinStrings(strings, " ", 1);
    }

    private static String joinStrings(String[] strings, String delimiter, int startIndex) {
        int length = strings.length;
        if (length == 0 ) return "";
        if (length < startIndex ) return "";
        StringBuilder words = new StringBuilder(strings[startIndex]);
        for (int i = startIndex + 1; i < length; i++) {
            words.append(delimiter).append(strings[i]);
        }
        return words.toString();
    }
}

using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.MessagePatterns;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace net_rabbitmq
{
    class Program
    {
        private const string HostName = "localhost";
        private const string UserName = "guest";
        private const string Password = "guest";
        private static Subscription _subscription2;
        private static Subscription _subscription3;
        static void Main(string[] args)
        {
            //createQueueAndExchange();
            //publishMsg();
            //useDurableQueue();
            //oneWayMessaging();
            //workerQueue();
            //pubSubMessaging();
            usingRPC();
        }

        private static void usingRPC()
        {
            var connFactory = new RabbitMQ.Client.ConnectionFactory()
            {
                UserName = UserName,
                Password = Password,
                HostName = HostName
            };
            var conn = connFactory.CreateConnection();
            var model = conn.CreateModel();

            model.QueueDeclare("rpcRequestQueue", false, false, false, null);
            Console.WriteLine("RPC Request Queue created");

            //creating dynamic response queue
            var conn2 = connFactory.CreateConnection();
            var model2 = conn2.CreateModel();

            var _responseQueue = model2.QueueDeclare().QueueName;
            Console.WriteLine("RPC Response Queue created " + _responseQueue);
            var _consumer = new QueueingBasicConsumer(model2);
            model2.BasicConsume(_responseQueue, true, _consumer);

            ThreadStart childref = new ThreadStart(rpcConsumer);
            Console.WriteLine("In Main: Creating the Child thread");
            Thread childThread = new Thread(childref);
            childThread.Start();

            var correlationToken = Guid.NewGuid().ToString();
            var properties = model2.CreateBasicProperties();
            properties.ReplyTo = _responseQueue;
            properties.CorrelationId = correlationToken;
            byte[] messageBuffer = Encoding.Default.GetBytes("rpc message");
            var timeoutAt = DateTime.Now + new TimeSpan(0, 0, 3, 0);
            model2.BasicPublish("", "rpcRequestQueue", properties, messageBuffer);
            while (DateTime.Now <= timeoutAt)
            {
                var deliveryArgs = (BasicDeliverEventArgs)_consumer.Queue.Dequeue();
                if (deliveryArgs.BasicProperties != null && deliveryArgs.BasicProperties.CorrelationId == correlationToken)
                {
                    var response = Encoding.Default.GetString(deliveryArgs.Body);
                    Console.WriteLine("Response - {0}", response);
                    model.BasicAck(deliveryArgs.DeliveryTag, false);
                }
            }

            Console.ReadLine();
        }

        private static void rpcConsumer()
        {
            var connFactory = new RabbitMQ.Client.ConnectionFactory()
            {
                UserName = UserName,
                Password = Password,
                HostName = HostName
            };
            var conn = connFactory.CreateConnection();
            var model = conn.CreateModel();
            model.BasicQos(0, 1, false);
            var consumer = new QueueingBasicConsumer(model);
            model.BasicConsume("rpcRequestQueue", false, consumer);
            var deliveryArgs = (BasicDeliverEventArgs)consumer.Queue.Dequeue();
            var message = Encoding.Default.GetString(deliveryArgs.Body);
            Console.WriteLine("RPC Consumer received: {0}", message);
            var response = string.Format("Processed message - {0} : Response is good", message);

            var replyProperties = model.CreateBasicProperties();
            replyProperties.CorrelationId = deliveryArgs.BasicProperties.CorrelationId;
            byte[] messageBuffer = Encoding.Default.GetBytes(response);
            model.BasicPublish("", deliveryArgs.BasicProperties.ReplyTo, replyProperties, messageBuffer);
            model.BasicAck(deliveryArgs.DeliveryTag, false);
        }

        private static void Poll2()
        {
            var connectionFactory = new ConnectionFactory
            {
                HostName = HostName,
                Password = Password,
                UserName = UserName
            };
            var connection2 = connectionFactory.CreateConnection();
            var model2 = connection2.CreateModel();
            model2.BasicQos(0, 1, false);//model2 processes 1 msg at a time, instead of a batch of msgs

            _subscription2 = new Subscription(model2, "pubSubMessagingQueue1");

            while (true) {
                var deliveryArgs = _subscription2.Next();
                var message = Encoding.Default.GetString(deliveryArgs.Body);
                Console.WriteLine("Message received at poll2 - {0}", message);
                _subscription2.Ack(deliveryArgs);
                Console.ReadLine();
            }
        }

        private static void Poll3()
        {
            var connectionFactory = new ConnectionFactory
            {
                HostName = HostName,
                Password = Password,
                UserName = UserName
            };
            var connection3 = connectionFactory.CreateConnection();
            var model3 = connection3.CreateModel();
            model3.BasicQos(0, 1, false);//model2 processes 1 msg at a time, instead of a batch of msgs

            _subscription3 = new Subscription(model3, "pubSubMessagingQueue2");

            while (true)
            {
                var deliveryArgs = _subscription3.Next();
                var message = Encoding.Default.GetString(deliveryArgs.Body);
                Console.WriteLine("Message received at poll3 - {0}", message);
                _subscription3.Ack(deliveryArgs);
                Console.ReadLine();
            }
        }

        static void pubSubMessaging()
        {
            Console.WriteLine("Starting RabbitMQ Message Sender");
            Console.WriteLine();
            var connectionFactory = new ConnectionFactory
            {
                HostName = HostName,
                Password = Password,
                UserName = UserName
            };
            var connection = connectionFactory.CreateConnection();
            var model = connection.CreateModel();

            model.ExchangeDeclare("pubSubMessagingExchange", ExchangeType.Fanout);

            model.QueueDeclare("pubSubMessagingQueue1", true, false, false, null);
            model.QueueBind("pubSubMessagingQueue1", "pubSubMessagingExchange", "");
            Console.WriteLine("Queue 1 created");

            model.QueueDeclare("pubSubMessagingQueue2", true, false, false, null);
            model.QueueBind("pubSubMessagingQueue2", "pubSubMessagingExchange", "");
            Console.WriteLine("Queue 2 created");

            Console.WriteLine("Starting RabbitMQ queue processor");
            Console.WriteLine();

            ThreadStart childref = new ThreadStart(Poll2);
            Console.WriteLine("In Main: Creating the Child thread");
            Thread childThread = new Thread(childref);
            childThread.Start();

            ThreadStart childref2 = new ThreadStart(Poll3);
            Console.WriteLine("In Main: Creating the Child thread");
            Thread childThread2 = new Thread(childref2);
            childThread2.Start();

            var properties = model.CreateBasicProperties();
            properties.Persistent = true;

            byte[] messageBuffer = Encoding.Default.GetBytes("this is a fanout message");
            model.BasicPublish("pubSubMessagingExchange", "", properties, messageBuffer);
            Console.WriteLine("Message sent");

            Console.ReadLine();
        }

        static void receiver2()
        {
            Console.WriteLine("Starting RabbitMQ queue processor 1");
            Console.WriteLine();
            var connectionFactory = new ConnectionFactory
            {
                HostName = HostName,
                Password = Password,
                UserName = UserName
            };
            var connection2 = connectionFactory.CreateConnection();
            var model2 = connection2.CreateModel();
            model2.BasicQos(0, 1, false);//model2 processes 1 msg at a time, instead of a batch of msgs

            var consumer = new QueueingBasicConsumer(model2);
            model2.BasicConsume("workerQueue", false, consumer);
            var deliveryArgs = (BasicDeliverEventArgs)consumer.Queue.Dequeue();
            var message = Encoding.Default.GetString(deliveryArgs.Body);
            Console.WriteLine("Message Received at queue processor 1 - {0}", message);
            model2.BasicAck(deliveryArgs.DeliveryTag, false);
        }

        static void receiver1()
        {
            Console.WriteLine("Starting RabbitMQ queue processor 2");
            Console.WriteLine();
            var connectionFactory = new ConnectionFactory
            {
                HostName = HostName,
                Password = Password,
                UserName = UserName
            };
            var connection1 = connectionFactory.CreateConnection();
            var model1 = connection1.CreateModel();
            model1.BasicQos(0, 1, false);//model2 processes 1 msg at a time, instead of a batch of msgs

            var consumer = new QueueingBasicConsumer(model1);
            model1.BasicConsume("workerQueue", false, consumer);
            var deliveryArgs = (BasicDeliverEventArgs)consumer.Queue.Dequeue();
            var message = Encoding.Default.GetString(deliveryArgs.Body);
            Console.WriteLine("Message Received at queue processor 2 - {0}", message);
            model1.BasicAck(deliveryArgs.DeliveryTag, false);
        }

        static void workerQueue()
        {
            Console.WriteLine("Starting RabbitMQ Message Sender");
            Console.WriteLine();
            var connectionFactory = new ConnectionFactory
            {
                HostName = HostName,
                Password = Password,
                UserName = UserName
            };
            var connection = connectionFactory.CreateConnection();
            var model = connection.CreateModel();

            //the queue will live through server restart
            model.QueueDeclare("workerQueue", true, false, false, null);
            Console.WriteLine("Queue created");

            var properties = model.CreateBasicProperties();
            properties.Persistent = true;

            ThreadStart childref = new ThreadStart(receiver1);
            Console.WriteLine("In Main: Creating the Child thread");
            Thread childThread = new Thread(childref);
            childThread.Start();

            ThreadStart childref2 = new ThreadStart(receiver2);
            Console.WriteLine("In Main: Creating the Child thread 2");
            Thread childThread2 = new Thread(childref2);
            childThread2.Start();

            byte[] messageBuffer = Encoding.Default.GetBytes("this is message");
            byte[] messageBuffer2 = Encoding.Default.GetBytes("this is message2");
            model.BasicPublish("", "workerQueue", properties, messageBuffer);
            model.BasicPublish("", "workerQueue", properties, messageBuffer2);
            Console.WriteLine("Two messages sent");

            Console.ReadLine();
        }

        static void oneWayMessaging()
        {
            Console.WriteLine("Starting RabbitMQ Message Sender");
            Console.WriteLine();
            var connectionFactory = new ConnectionFactory
            {
                HostName = HostName,
                Password = Password,
                UserName = UserName
            };
            var connection = connectionFactory.CreateConnection();
            var model = connection.CreateModel();

            //the queue will live through server restart
            model.QueueDeclare("oneWayMessaging", true, false, false, null);
            Console.WriteLine("Queue created");

            var properties = model.CreateBasicProperties();
            properties.Persistent = true;

            byte[] messageBuffer = Encoding.Default.GetBytes("this is a one-way message");
            model.BasicPublish("", "oneWayMessaging", properties, messageBuffer);
            Console.WriteLine("Message sent");

            Console.WriteLine("Starting RabbitMQ queue processor");
            Console.WriteLine();

            var connection2 = connectionFactory.CreateConnection();
            var model2 = connection2.CreateModel();
            model2.BasicQos(0, 1, false);//model2 processes 1 msg at a time, instead of a batch of msgs

            var consumer = new QueueingBasicConsumer(model2);
            model2.BasicConsume("oneWayMessaging", false, consumer);
            var deliveryArgs = (BasicDeliverEventArgs)consumer.Queue.Dequeue();
            var message = Encoding.Default.GetString(deliveryArgs.Body);
            Console.WriteLine("Message Received - {0}", message);
            model2.BasicAck(deliveryArgs.DeliveryTag, false);
            Console.ReadLine();
        }

        static void useDurableQueue()
        {
            Console.WriteLine("Starting RabbitMQ Message Sender");
            Console.WriteLine();
            Console.WriteLine();
            var connectionFactory = new ConnectionFactory
            {
                HostName = HostName,
                Password = Password,
                UserName = UserName
            };
            var connection = connectionFactory.CreateConnection();
            var model = connection.CreateModel();

            //the queue will live through server restart
            model.QueueDeclare("Module1.Sample4", true, false, false, null);
            Console.WriteLine("Queue created");

            var properties = model.CreateBasicProperties();
            properties.Persistent = false;//see if you want the message live through server restart

            byte[] messageBuffer = Encoding.Default.GetBytes("this is my message");
            model.BasicPublish("", "Module1.Sample4", properties, messageBuffer);
            Console.WriteLine("Message sent");
            Console.ReadLine();
        }

        static void publishMsg()
        {
            Console.WriteLine("Starting RabbitMQ Message Sender");
            Console.WriteLine();
            Console.WriteLine();
            var connectionFactory = new ConnectionFactory
            {
                HostName = HostName,
                Password = Password,
                UserName = UserName
            };
            var connection = connectionFactory.CreateConnection();
            var model = connection.CreateModel();

            model.QueueDeclare("Module1.Sample3", false, false, false, null);
            Console.WriteLine("Queue created");

            var properties = model.CreateBasicProperties();
            properties.Persistent = false;

            byte[] messageBuffer = Encoding.Default.GetBytes("this is my message");
            model.BasicPublish("", "Module1.Sample3", properties, messageBuffer);
            Console.WriteLine("Message sent");
            Console.ReadLine();
        }

        static void createQueueAndExchange()
        {
            var connFactory = new RabbitMQ.Client.ConnectionFactory()
            {
                UserName = UserName,
                Password = Password,
                HostName = HostName
            };
            var conn = connFactory.CreateConnection();
            var model = conn.CreateModel();

            model.QueueDeclare("MyQueue2", true, false, false, null);
            Console.WriteLine("Queue created");

            model.ExchangeDeclare("MyExchange2", ExchangeType.Topic);
            Console.WriteLine("Exchange created");

            model.QueueBind("MyQueue2", "MyExchange2", "cars");
            Console.WriteLine("Exchange and queue are bound");

            Console.ReadLine();
        }
    }
}

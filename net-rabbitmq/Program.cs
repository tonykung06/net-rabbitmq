using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace net_rabbitmq
{
    class Program
    {
        private const string HostName = "localhost";
        private const string UserName = "guest";
        private const string Password = "guest";
        static void Main(string[] args)
        {
            //createQueueAndExchange();
            //publishMsg();
            //useDurableQueue();
            oneWayMessaging();
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

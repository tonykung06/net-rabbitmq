using RabbitMQ.Client;
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
            useDurableQueue();
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

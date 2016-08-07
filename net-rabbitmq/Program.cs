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

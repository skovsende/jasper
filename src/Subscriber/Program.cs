using System;
using Jasper;
using Jasper.Bus.Transports.Configuration;
using Jasper.CommandLine;
using TestMessages;

namespace Subscriber
{
    class Program
    {
        static int Main(string[] args)
        {
            return JasperAgent.Run<SubscriberApp>();
        }
    }

    public class SubscriberApp : JasperRegistry
    {
        public SubscriberApp()
        {
            Subscribe.At("http://loadbalancer/messages");
            Subscribe.ToAllMessages();

            Transports.LightweightListenerAt(22222);
        }
    }


    public class UserHandler
    {
        public void Handle(UserCreated newGuy)
        {
            Console.Out.WriteLine($"User Created: {newGuy.UserId}");
        }

        public void Handle(UserDeleted deleted)
        {
            Console.Out.WriteLine($"User Deleted: {deleted.UserId}");
        }
    }
}

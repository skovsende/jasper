using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Jasper.Util;

namespace Jasper.Bus.Transports.Core
{

    public class ListeningAgent : IDisposable
    {
        public int Port { get; }
        private readonly CancellationToken _cancellationToken;
        private readonly TcpListener _listener;
        private Task _receivingLoop;
        private readonly ActionBlock<Socket> _socketHandling;
        private readonly Uri _uri;

        public ListeningAgent(ITcpReceiverCallback callback, int port, string protocol, CancellationToken cancellationToken)
        {
            Port = port;
            _cancellationToken = cancellationToken;
            _listener = new TcpListener(new IPEndPoint(IPAddress.Loopback, port));
            _listener.Server.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);


            _socketHandling = new ActionBlock<Socket>(async s =>
            {
                using (var stream = new NetworkStream(s, true))
                {
                    await WireProtocol.Receive(stream, callback, _uri);
                }
            });
            //TODO: I think we want to set the degree of parallelism here, otherwise, we're only reading from one socket at a time and waiting for those messages to be persisted

            _uri = $"{protocol}://{Environment.MachineName}:{port}/".ToUri();

        }

        public void Start()
        {
            _receivingLoop = Task.Run(async () =>
            {
                _listener.Start();

                while (!_cancellationToken.IsCancellationRequested)
                {
                    var socket = await _listener.AcceptSocketAsync();
                    _socketHandling.Post(socket);
                }
            }, _cancellationToken);
        }

        public void Dispose()
        {
            _socketHandling.Complete();
            _listener.Stop();
            _listener.Server.Dispose();
        }
    }
}

using System;
using System.Threading;
using System.Threading.Tasks;
using Jasper.Bus.Runtime;

namespace Jasper.Bus.Transports.Core
{
    public class TcpQueueMessageCallback : IMessageCallback
    {
        private readonly Envelope _envelope;
        private readonly TcpQueue _tcpQueue;
        private readonly CancellationToken _cancellationToken;

        public TcpQueueMessageCallback(Envelope envelope, TcpQueue tcpQueue, CancellationToken cancellationToken)
        {
            _envelope = envelope;
            _tcpQueue = tcpQueue;
            _cancellationToken = cancellationToken;
        }

        public Task MarkSuccessful()
        {
            return _tcpQueue.Receive(_envelope, _cancellationToken);
        }

        public Task MarkFailed(Exception ex)
        {
            //TODO: It doesn't seem like MarkFailed should continue to be a thing; either retry or move to errors... unless _this is_ supposed to be retry
            return _tcpQueue.Receive(_envelope, _cancellationToken);
        }

        public Task MoveToErrors(ErrorReport report)
        {
            // There's an outstanding issue for actually doing error reports
            return _tcpQueue.MoveToError(_envelope, report);
        }

        public Task Requeue(Envelope envelope)
        {
            return _tcpQueue.Requeue(_envelope, envelope);
        }
    }
}

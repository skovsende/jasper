using System;
using System.Threading.Tasks;
using Jasper.Bus.Runtime;

namespace Jasper.Bus.Transports.Core
{
    public interface ITcpReceiverCallback
    {
        Task<ReceivedStatus> Received(Uri uri, Envelope[] messages);
        void Acknowledged(Envelope[] messages);
        void NotAcknowledged(Envelope[] messages);
        void Failed(Exception exception, Envelope[] messages);
    }
}

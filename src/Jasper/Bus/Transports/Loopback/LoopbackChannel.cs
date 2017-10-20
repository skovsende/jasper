using System.Threading.Tasks;
using Jasper.Bus.Runtime;
using Jasper.Bus.Transports.Configuration;
using Jasper.Bus.Transports.Core;

namespace Jasper.Bus.Transports.Loopback
{
    public class LoopbackChannel : ChannelBase
    {
        private readonly LoopbackTransport _transport;

        public LoopbackChannel(SubscriberAddress address, LoopbackTransport transport) : base(address, TransportConstants.RepliesUri)
        {
            _transport = transport;
        }

        protected override Task send(Envelope envelope)
        {
            envelope.ReceivedAt = Destination;
            return _transport.Send(envelope, Destination);
        }
    }
}

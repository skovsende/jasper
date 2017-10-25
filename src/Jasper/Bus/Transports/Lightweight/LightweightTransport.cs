using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Jasper.Bus.Logging;
using Jasper.Bus.Transports.Configuration;
using Jasper.Bus.Transports.Core;

namespace Jasper.Bus.Transports.Lightweight
{
    public class LightweightTransport : TransportBase
    {
        public static readonly string ProtocolName = "tcp";

        public LightweightTransport(CompositeLogger logger, BusSettings settings)
            : base(ProtocolName, new NulloPersistence(), logger, new SocketSenderProtocol(), settings)
        {
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Baseline;
using Jasper.Bus.Configuration;
using Jasper.Bus.Logging;
using Jasper.Bus.Runtime.Routing;
using Jasper.Bus.Transports;
using Jasper.Bus.Transports.Configuration;

namespace Jasper.Bus.Runtime
{
    public class EnvelopeSender : IEnvelopeSender
    {
        private readonly IMessageRouter _router;
        private readonly IChannelGraph _channels;
        private readonly UriAliasLookup _aliases;
        private readonly BusSettings _settings;
        private readonly IDictionary<string, ITransport> _transports = new Dictionary<string, ITransport>();


        public EnvelopeSender(CompositeLogger logger, IMessageRouter router, IChannelGraph channels, UriAliasLookup aliases, BusSettings settings, IEnumerable<ITransport> transports)
        {
            _router = router;
            _channels = channels;
            _aliases = aliases;
            _settings = settings;

            foreach (var transport in transports)
            {
                _transports.SmartAdd(transport.Protocol, transport);
            }

            Logger = logger;
        }

        public IBusLogger Logger { get;}

        public async Task<string> Send(Envelope envelope)
        {
            if (envelope.Message == null) throw new ArgumentNullException(nameof(envelope.Message));

            envelope.Source = _settings.NodeId;

            if (envelope.Destination == null)
            {
                var routes = await _router.Route(envelope.Message.GetType());
                if (!routes.Any())
                {
                    Logger.NoRoutesFor(envelope);

                    if (_settings.NoMessageRouteBehavior == NoRouteBehavior.ThrowOnNoRoutes)
                    {
                        throw new NoRoutesException(envelope);
                    }
                }

                foreach (var route in routes)
                {
                    await sendEnvelope(envelope, route);
                }
            }
            else
            {
                var route = await _router.RouteForDestination(envelope);
                await sendEnvelope(envelope, route);
            }

            return envelope.CorrelationId;
        }

        public Task EnqueueLocally(object message)
        {
            var channel = _channels.DefaultChannel;
            var envelope = new Envelope
            {
                Message = message,
                Destination = channel.Uri
            };

            return Send(envelope);
        }

        private async Task<Envelope> sendEnvelope(Envelope envelope, MessageRoute route)
        {
            if (route == null) throw new ArgumentNullException(nameof(route));

            ITransport transport = null;
            if (_transports.TryGetValue(route.Destination.Scheme, out transport))
            {
                var sending = route.CloneForSending(envelope);

                var channel = _channels.TryGetChannel(route.Destination);

                if (channel != null)
                {
                    await sendToStaticChannel(sending, channel);
                }
                else
                {
                    await sendToDynamicChannel(route.Destination, sending, transport);
                }

                Logger.Sent(sending);

                return sending;
            }
            else
            {
                throw new InvalidOperationException($"Unrecognized transport scheme '{route.Destination.Scheme}'");
            }
        }

        private static async Task sendToDynamicChannel(Uri address, Envelope sending, ITransport transport)
        {
            sending.Destination = address;
            sending.ReplyUri = transport.DefaultReplyUri();

            await transport.Send(sending, sending.Destination).ConfigureAwait(false);
        }

        private static async Task sendToStaticChannel(Envelope sending, IChannel channel)
        {
            sending.Destination = channel.Destination;
            sending.ReplyUri = channel.ReplyUri;

            await channel.Send(sending).ConfigureAwait(false);
        }

    }
}

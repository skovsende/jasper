using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Jasper.Bus;
using Jasper.Bus.Runtime;
using Jasper.Bus.Runtime.Routing;
using Marten;
using Marten.Services;

namespace Jasper.Marten.Persistence
{
    public class OutboxListener : DocumentSessionListenerBase
    {
        private readonly IMessageRouter _router;
        private readonly IDictionary<string, IChannel> _channels = new Dictionary<string, IChannel>();

        public OutboxListener(IMessageRouter router)
        {
            _router = router;
        }

        public override async Task BeforeSaveChangesAsync(IDocumentSession session, CancellationToken token)
        {
            var envelopes = session.PendingChanges.AllChangedFor<Envelope>();

            foreach (var envelope in envelopes)
            {
                session.Eject(envelope);
                await determineOutgoingEnvelopesForMessage(session, envelope);
            }


        }

        public override Task AfterCommitAsync(IDocumentSession session, IChangeSet commit, CancellationToken token)
        {
            foreach (var envelope in commit.Updated.OfType<Envelope>())
            {
                 _channels[envelope.Id].EnqueueFromOutbox(envelope);
            }

            return Task.CompletedTask;
        }

        // This might be the only part you care about
        private async Task determineOutgoingEnvelopesForMessage(IDocumentSession session, Envelope envelope)
        {
            var routes = await _router.Route(envelope.Message.GetType());
            foreach (var route in routes)
            {
                var outgoing = route.CloneForSending(envelope);
                outgoing.EnsureData(); // gotta do that for serialization before persisting. Sad trombone.
                                       // You don't want to have to rely on Newtonsoft to serialize as a nested
                                       // object



                session.Store(outgoing);
            }
        }


    }
}

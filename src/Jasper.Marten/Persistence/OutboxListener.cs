using System.Threading;
using System.Threading.Tasks;
using Jasper.Bus.Runtime;
using Jasper.Bus.Runtime.Routing;
using Marten;

namespace Jasper.Marten
{
    public class OutboxListener : DocumentSessionListenerBase
    {
        private readonly IMessageRouter _router;

        public OutboxListener(IMessageRouter router)
        {
            _router = router;
        }

        public override async Task BeforeSaveChangesAsync(IDocumentSession session, CancellationToken token)
        {
            var envelopes = session.PendingChanges.AllChangedFor<Envelope>();

            // TODO -- Marten needs an Eject() operation. Been requested, so might as well do it
            // But you'd have to eject the original envelopes here.


            foreach (var envelope in envelopes)
            {
                await determineOutgoingEnvelopesForMessage(session, envelope);
            }


        }

        private async Task determineOutgoingEnvelopesForMessage(IDocumentSession session, Envelope envelope)
        {
            var routes = await _router.Route(envelope.Message.GetType());
            foreach (var route in routes)
            {
                var outgoing = route.CloneForSending(envelope);
                outgoing.EnsureData(); // gotta do that for serialization before persisting. Sad trombone.
                session.Store(outgoing);
            }
        }
    }
}

using System;
using System.Threading.Tasks;
using Jasper.Bus.Runtime;
using Marten;

namespace Jasper.Marten
{
    public static class SessionOutboxExtensions
    {
        public static void EnqueueOutgoingMessage(this IDocumentSession session, object message, Action<Envelope> customizeSending = null)
        {
            var envelope = new Envelope(message)
            {
                Position = EnvelopePosition.StagedForOutgoing
            };

            customizeSending?.Invoke(envelope);

            session.Store(envelope);
        }

        // I think this would mostly be used by middleware
        public static Task SendAllPersistedEnvelopes(this IEnvelopeSender sender, IDocumentSession session)
        {
            session.
        }
    }
}

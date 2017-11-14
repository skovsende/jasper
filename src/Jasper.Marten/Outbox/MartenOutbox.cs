using System;
using System.Threading.Tasks;
using Jasper.Bus.Runtime;
using Jasper.Bus.Runtime.Routing;
using Marten;

namespace Jasper.Marten.Outbox
{
    public class MartenOutbox : IMartenOutbox
    {
        private readonly Func<IDocumentSession> _sessionFactory;
        private readonly OutboxCommitListener _listener;
        private readonly IMessageRouter _router;

        private IDocumentSession _trackedSession;

        public MartenOutbox(
            Func<IDocumentSession> sessionFactory,
            OutboxCommitListener listener,
            IMessageRouter router)
        {
            _sessionFactory = sessionFactory;
            _listener = listener;
            _router = router;
        }

        /// <summary>
        /// Causes all the bus messages sent via this object to be included in the unit of work represented by a call to session.SaveChanges() (or SaveChangesAsync)
        /// </summary>
        /// <exception cref="InvalidOperationException">If Enlist is called after a DocumentSession has already been accessed</exception>
        public void Enlist(IDocumentSession session)
        {
            if (_trackedSession != null)
                throw new InvalidOperationException(
                    "This outbox is already associated with a document session. Call Enlist before Accessing the DocumentSession property or sending any messages.");
            _trackedSession = session;
        }

        /// <summary>
        /// Provides access to the document session this outbox is tracking. If this has not enlisted with an existing session, a new one is created by the sessionFactory
        /// </summary>
        public IDocumentSession DocumentSession
        {
            get
            {
                if (_trackedSession == null)
                {
                    _trackedSession = _sessionFactory.Invoke();
                }
                return _trackedSession;
            }
        }

        public async Task Send<T>(T message)
        {
            var routes = await _router.Route(typeof(T));
            Envelope envelope = new Envelope { Message = message };
            foreach (var route in routes)
            {
                var outgoing = route.CloneForSending(envelope);
                outgoing.EnsureData();
                // Tell the listener that after this session is committed it
                // needs to tell the sending agent to enqueue the envelope.
                var storedEnvelope = new StoredEnvelope(outgoing, "outgoing");
                _listener.DeliverEnvelopeAfterCommit(DocumentSession, storedEnvelope);
                DocumentSession.Store(storedEnvelope);
            }
        }
    }

    public interface IMartenOutbox
    {
        /// <summary>
        /// Provides access to the document session this outbox is tracking. If this has not enlisted with an existing session, a new one is created by the sessionFactory
        /// </summary>
        IDocumentSession DocumentSession { get; }

        Task Send<T>(T message);
    }
}

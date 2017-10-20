using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Jasper.Bus.Runtime;

namespace Jasper.Bus.Transports.Core
{
    //TODO: make these interfaces async
    public interface IPersistence : IInboxPersistence, IOutboxPersistence
    {
        void Remove(string queueName, IEnumerable<Envelope> envelopes);
        void Remove(Envelope[] messages);

        void ClearAllStoredMessages(string queuePath = null);
        Task<bool> TryClaim(Envelope candidateEnvelope);
    }

    public interface IInboxPersistence
    {
        void Initialize(string[] queueNames);
        Task<IEnumerable<Envelope>> LoadInbox(string[] queueNames, CancellationToken cancellationToken);
        void StoreInitial(Envelope[] messages);
        void Remove(string queueName, Envelope envelope);
        void Replace(string queueName, Envelope envelope);
    }

    public interface IOutboxPersistence
    {
        void RemoveOutgoing(IList<Envelope> outgoingMessages);
        void PersistBasedOnSentAttempts(OutgoingMessageBatch batch, int maxAttempts);
        void StoreOutgoing(Envelope envelope);
        void RecoverOutgoingMessages(Action<Envelope> action, CancellationToken cancellation);
    }

    public class NulloPersistence : IPersistence
    {
        public void Remove(string queueName, IEnumerable<Envelope> envelopes)
        {
        }

        public void Remove(string queueName, Envelope envelope)
        {
        }

        public void Replace(string queueName, Envelope envelope)
        {
        }

        public Task<IEnumerable<Envelope>> LoadInbox(string[] queueNames, CancellationToken cancellationToken)
        {
            return Task.FromResult(Enumerable.Empty<Envelope>());
        }

        public void StoreInitial(Envelope[] messages)
        {
        }

        public void Remove(Envelope[] messages)
        {
        }

        public void RemoveOutgoing(IList<Envelope> outgoingMessages)
        {
        }

        public void PersistBasedOnSentAttempts(OutgoingMessageBatch batch, int maxAttempts)
        {
        }

        public void Initialize(string[] queueNames)
        {
        }

        public void StoreOutgoing(Envelope envelope)
        {
        }

        public void ClearAllStoredMessages(string queuePath = null)
        {

        }

        public Task<bool> TryClaim(Envelope candidateEnvelope)
        {
            return Task.FromResult(true);
        }

        public void RecoverOutgoingMessages(Action<Envelope> action, CancellationToken cancellation)
        {

        }
    }
}

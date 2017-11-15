using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Jasper.Bus;
using Jasper.Bus.Runtime;
using Marten;
using Marten.Services;

namespace Jasper.Marten.Outbox
{
    /// <summary>
    /// This class is used by the MartenOutbox class to react to the commit of sessions it attaches to.
    /// </summary>
    public class OutboxCommitListener : DocumentSessionListenerBase
    {
        private readonly IChannelGraph _channels;

        private readonly ConcurrentDictionary<IDocumentSession, HashSet<Envelope>> _envelopesToWatchFor
            = new ConcurrentDictionary<IDocumentSession, HashSet<Envelope>>();

        public OutboxCommitListener(IChannelGraph channels)
        {
            _channels = channels;
        }

        public override void AfterCommit(IDocumentSession session, IChangeSet commit)
        {
            enqueueInsertedOutboxEnvelopes(session, commit);
        }

        public override Task AfterCommitAsync(IDocumentSession session, IChangeSet commit, CancellationToken token)
        {
            enqueueInsertedOutboxEnvelopes(session, commit);
            return Task.CompletedTask;
        }

        private void enqueueInsertedOutboxEnvelopes(IDocumentSession session, IChangeSet commit)
        {
            // if we've been informed by the outbox of any envelopes, look for them in the updated objects and queue them for delivery.
            // apparently the envelopes are considered updated because they already have an Id property assigned?
            if (_envelopesToWatchFor.TryRemove(session, out var envelopes))
            {
                foreach (var insertedObject in commit.Updated)
                {
                    if (insertedObject is Envelope envelope)
                    {
                        if (envelopes.Contains(envelope))
                        {
                            _channels.GetOrBuildChannel(envelope.Destination).EnqueueFromOutbox(envelope);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Informs the listener to enqueue the envelope for delivery after the
        /// given session is committed with the envelope inserted.
        /// </summary>
        public void DeliverEnvelopeAfterCommit(IDocumentSession session, Envelope envelope)
        {
            // Just in case somebody implements Envelope.Equals and
            // Envelope.GetHashCode or implements IEquatable, we actually don't
            // want to use them. We always want reference equality.
            var list = _envelopesToWatchFor.GetOrAdd(session, s => new HashSet<Envelope>(ReferenceEqualityComparer.Default));
            // Note: we have one HashSet per IDocumentSession and expect any
            // given IDocumentSession instance to be used by only one thread
            // at a time, so we're not synchronizing access to the HashSet,
            // just the dictionary that's shared by all sessions.
            list.Add(envelope);
        }

        private sealed class ReferenceEqualityComparer : IEqualityComparer<object>
        {
            public static ReferenceEqualityComparer Default { get; } = new ReferenceEqualityComparer();

            bool IEqualityComparer<object>.Equals(object x, object y) => ReferenceEquals(x, y);

            public int GetHashCode(object obj) => RuntimeHelpers.GetHashCode(obj);
        }
    }
}

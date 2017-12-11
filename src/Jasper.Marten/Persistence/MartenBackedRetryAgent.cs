using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Baseline;
using Jasper.Bus.Runtime;
using Jasper.Bus.Transports;
using Jasper.Bus.Transports.Configuration;
using Jasper.Bus.Transports.Sending;
using Jasper.Bus.Transports.Tcp;
using Jasper.Marten.Persistence.Resiliency;
using Marten;
using Marten.Util;
using NpgsqlTypes;

namespace Jasper.Marten.Persistence
{
    public class MartenBackedRetryAgent : RetryAgent
    {
        private readonly IDocumentStore _store;
        private readonly OwnershipMarker _marker;
        private string _deleteIncoming;

        public MartenBackedRetryAgent(IDocumentStore store, ISender sender, RetrySettings settings, OwnershipMarker marker) : base(sender, settings)
        {
            _store = store;
            _marker = marker;

            _deleteIncoming = $"delete from {_marker.Incoming} where id = ANY(:idlist)";
        }

        // TODO -- add logging for envelopes discarded
        public override async Task EnqueueForRetry(OutgoingMessageBatch batch)
        {

            var expiredInQueue = Queued.Where(x => x.IsExpired()).ToArray();
            var expiredInBatch = batch.Messages.Where(x => x.IsExpired()).ToArray();


            try
            {
                using (var session = _store.LightweightSession())
                {
                    foreach (var envelope in expiredInBatch.Concat(expiredInQueue))
                    {
                        session.Delete(envelope);
                    }

                    var all = Queued.Where(x => !expiredInQueue.Contains(x))
                        .Concat(batch.Messages.Where(x => !expiredInBatch.Contains(x)))
                        .ToList();

                    if (all.Count > _settings.MaximumEnvelopeRetryStorage)
                    {
                        var reassigned = all.Skip(_settings.MaximumEnvelopeRetryStorage).ToArray();
                        await _marker.MarkIncomingOwnedByAnyNode(session, reassigned);
                    }

                    await session.SaveChangesAsync();

                    Queued = all.Take(_settings.MaximumEnvelopeRetryStorage).ToList();
                }
            }
            catch (Exception)
            {
                // Put these back

                // TODO -- FAR BETTER STRATEGY HERE!
                Thread.Sleep(100);
                await EnqueueForRetry(batch);
            }
        }

        public IList<Envelope> Queued { get; private set; } = new List<Envelope>();

        protected override void afterRestarting(ISender sender)
        {
            var expired = Queued.Where(x => x.IsExpired());
            if (expired.Any())
            {
                using (var conn = _store.Tenancy.Default.CreateConnection())
                {
                    conn.Open();

                    conn.CreateCommand(_deleteIncoming)
                        .With("idlist", expired.Select(x => x.Id).ToArray(), NpgsqlDbType.Array | NpgsqlDbType.Uuid)
                        .ExecuteNonQuery();
                }
            }

            var toRetry = Queued.Where(x => !x.IsExpired()).ToArray();
            Queued = new List<Envelope>();

            foreach (var envelope in toRetry)
            {
                // It's perfectly okay to not wait on the task here
                _sender.Enqueue(envelope);
            }


        }
    }
}

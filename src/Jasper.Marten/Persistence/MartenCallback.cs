using System;
using System.Threading.Tasks;
using Jasper.Bus.Runtime;
using Jasper.Bus.Transports;
using Jasper.Bus.WorkerQueues;
using Jasper.Marten.Persistence.Resiliency;
using Marten;
using Marten.Util;
using NpgsqlTypes;

namespace Jasper.Marten.Persistence
{
    public class MartenCallback : IMessageCallback
    {
        private readonly Envelope _envelope;
        private readonly IWorkerQueue _queue;
        private readonly IDocumentStore _store;
        private readonly OwnershipMarker _marker;

        public MartenCallback(Envelope envelope, IWorkerQueue queue, IDocumentStore store, OwnershipMarker marker)
        {
            _envelope = envelope;
            _queue = queue;
            _store = store;
            _marker = marker;
        }

        public async Task MarkComplete()
        {
            // TODO -- later, come back and do retries?

            using (var conn = _store.Tenancy.Default.CreateConnection())
            {
                await conn.OpenAsync();

                await conn.CreateCommand($"delete from {_marker.Incoming} where id = :id")
                    .With("id", _envelope.Id, NpgsqlDbType.Uuid)
                    .ExecuteNonQueryAsync();
            }
        }

        public async Task MoveToErrors(Envelope envelope, Exception exception)
        {
            // TODO -- later, come back and do retries?


            using (var session = _store.LightweightSession())
            {
                var connection = session.Connection;
                await connection.CreateCommand($"delete from {_marker.Incoming} where id = :id")
                    .With("id", envelope.Id, NpgsqlDbType.Uuid)
                    .ExecuteNonQueryAsync();

                var report = new ErrorReport(envelope, exception);
                session.Store(report);

                await session.SaveChangesAsync();
            }

            // TODO -- make this configurable about whether or not it saves off error reports

        }

        public async Task Requeue(Envelope envelope)
        {
            envelope.Attempts++;

            using (var conn = _store.Tenancy.Default.CreateConnection())
            {
                await conn.OpenAsync();

                await conn.CreateCommand($"update {_marker.Incoming} set attempts = :attempts where id = :id")
                    .With("attempts", envelope.Attempts, NpgsqlDbType.Integer)
                    .With("id", envelope.Id, NpgsqlDbType.Uuid)
                    .ExecuteNonQueryAsync();
            }

            await _queue.Enqueue(envelope);
        }

        public async Task MoveToDelayedUntil(DateTime time, Envelope envelope)
        {
            envelope.ExecutionTime = time;
            envelope.Status = TransportConstants.Scheduled;

            using (var session = _store.LightweightSession())
            {
                session.StoreIncoming(_marker, envelope);

                await session.SaveChangesAsync();
            }
        }
    }
}

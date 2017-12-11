using System.Collections.Generic;
using System.Threading.Tasks;
using Jasper.Bus.Runtime;
using Jasper.Marten.Persistence.Resiliency;
using Marten;
using Npgsql;

namespace Jasper.Marten.Persistence
{
    public static class MartenStorageExtensions
    {
        public static async Task<List<Envelope>> ExecuteToEnvelopes(this NpgsqlCommand command)
        {
            using (var reader = await command.ExecuteReaderAsync())
            {
                var list = new List<Envelope>();

                while (await reader.ReadAsync())
                {
                    var bytes = await reader.GetFieldValueAsync<byte[]>(0);
                    var envelope = Envelope.Read(bytes);

                    list.Add(envelope);
                }

                return list;
            }
        }

        public static void StoreIncoming(this IDocumentSession session, OwnershipMarker marker, Envelope envelope)
        {
            var operation = new StoreIncomingEnvelope(marker.Incoming, envelope);
            session.QueueOperation(operation);
        }

        public static void StoreIncoming(this IDocumentSession session, OwnershipMarker marker, Envelope[] messages)
        {
            foreach (var envelope in messages)
            {
                var operation = new StoreIncomingEnvelope(marker.Incoming, envelope);
                session.QueueOperation(operation);
            }
        }
    }
}
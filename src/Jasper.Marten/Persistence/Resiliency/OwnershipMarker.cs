using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Jasper.Bus.Runtime;
using Jasper.Bus.Transports;
using Jasper.Bus.Transports.Configuration;
using Jasper.Internals.Codegen;
using Jasper.Util;
using Marten;
using Marten.Schema;
using Marten.Util;
using NpgsqlTypes;

namespace Jasper.Marten.Persistence.Resiliency
{
    // TODO -- use Marten operations instead of direct SQL calls here to batch up commands
    public class OwnershipMarker
    {
        private readonly string _markOwnedOutgoingSql;
        private readonly string _markOwnedIncomingSql;






        public OwnershipMarker(BusSettings settings, StoreOptions storeConfiguration)
        {
            Incoming = new DbObjectName(storeConfiguration.DatabaseSchemaName,
                PostgresqlEnvelopeStorage.IncomingTableName);
            Outgoing = new DbObjectName(storeConfiguration.DatabaseSchemaName,
                PostgresqlEnvelopeStorage.OutgoingTableName);


            _markOwnedOutgoingSql = $"update {Outgoing} set owner_id = :owner where id = ANY(:idlist)";



            _markOwnedIncomingSql = $"update {Incoming} set owner_id = :owner where id = ANY(:idlist)";

            CurrentNodeId = settings.UniqueNodeId;










        }

        public int CurrentNodeId { get; }

        public DbObjectName Incoming { get; }

        public DbObjectName Outgoing { get; }




        // TODO -- make this be an operation
        public Task MarkIncomingOwnedByThisNode(IDocumentSession session, List<Envelope> envelopes)
        {
            var identities = envelopes.Select(x => x.Id).ToArray();

            return session.Connection.CreateCommand()
                .Sql(_markOwnedIncomingSql)
                .With("idlist", identities, NpgsqlDbType.Array | NpgsqlDbType.Uuid)
                .With("owner", CurrentNodeId, NpgsqlDbType.Integer)
                .ExecuteNonQueryAsync();
        }

        // TODO -- convert this to an operation
        public Task MarkOutgoingOwnedByThisNode(IDocumentSession session, params Envelope[] envelopes)
        {
            var identities = envelopes.Select(x => x.Id).ToArray();

            return session.Connection.CreateCommand()
                .Sql(_markOwnedOutgoingSql)
                .With("idlist", identities, NpgsqlDbType.Array | NpgsqlDbType.Uuid)
                .With("status", TransportConstants.Outgoing, NpgsqlDbType.Varchar)
                .With("owner", CurrentNodeId, NpgsqlDbType.Integer)
                .ExecuteNonQueryAsync();
        }

        // TODO -- convert to an operation
        public Task MarkIncomingOwnedByAnyNode(IDocumentSession session, params Envelope[] envelopes)
        {
            var identities = envelopes.Select(x => x.Id).ToArray();

            return session.Connection.CreateCommand()
                .Sql(_markOwnedIncomingSql)
                .With("idlist", identities, NpgsqlDbType.Array | NpgsqlDbType.Uuid)
                .With("owner", TransportConstants.AnyNode, NpgsqlDbType.Integer)
                .ExecuteNonQueryAsync();
        }










    }
}

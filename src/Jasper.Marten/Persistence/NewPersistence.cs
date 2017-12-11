using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using Jasper.Bus.Runtime;
using Jasper.Marten.Persistence.Resiliency;
using Marten;
using Marten.Schema;
using Marten.Services;
using Marten.Storage;
using Marten.Util;
using Npgsql;
using NpgsqlTypes;

namespace Jasper.Marten.Persistence
{
    public class PostgresqlEnvelopeStorage : FeatureSchemaBase
    {
        public const string IncomingTableName = "mt_envelopes_incoming";
        public const string OutgoingTableName = "mt_envelopes_outgoing";

        public PostgresqlEnvelopeStorage(StoreOptions options) : base("envelope-storage", options)
        {
        }

        protected override IEnumerable<ISchemaObject> schemaObjects()
        {
            yield return new IncomingEnvelopeTable(Options);
            yield return new OutgoingEnvelopeTable(Options);
        }

        public override Type StorageType => typeof(Envelope);
    }

    public class IncomingEnvelopeTable : Table
    {
        public IncomingEnvelopeTable(StoreOptions options) : base(new DbObjectName(options.DatabaseSchemaName, PostgresqlEnvelopeStorage.IncomingTableName))
        {
            AddPrimaryKey(new TableColumn("id", "uuid"));
            AddColumn("status", "varchar", "NOT NULL");
            AddColumn("owner_id", "int", "NOT NULL");
            AddColumn("execution_time", "timestamp");
            AddColumn("attempts", "int", "DEFAULT 0");
            AddColumn("body", "bytea", "NOT NULL");
        }
    }

    public class OutgoingEnvelopeTable : Table
    {
        public OutgoingEnvelopeTable(StoreOptions options) : base(new DbObjectName(options.DatabaseSchemaName, PostgresqlEnvelopeStorage.OutgoingTableName))
        {
            AddPrimaryKey(new TableColumn("id", "uuid"));
            AddColumn("owner_id", "int", "NOT NULL");
            AddColumn("destination", "varchar");
            AddColumn("deliver_by", "timestamp");
            AddColumn("body", "bytea", "NOT NULL");
        }
    }

    public class StoreIncomingEnvelope : IStorageOperation
    {
        public Envelope Envelope { get; }
        private readonly DbObjectName _incomingTable;

        public StoreIncomingEnvelope(DbObjectName incomingTable, Envelope envelope)
        {
            // TODO -- do some assertions here

            Envelope = envelope;
            _incomingTable = incomingTable;
        }

        public void ConfigureCommand(CommandBuilder builder)
        {
            Envelope.EnsureData();
            var bytes = Envelope.Serialize();

            var id = builder.AddParameter(Envelope.Id, NpgsqlDbType.Uuid);
            var owner = builder.AddParameter(Envelope.OwnerId, NpgsqlDbType.Integer);
            var status = builder.AddParameter(Envelope.Status, NpgsqlDbType.Varchar);
            var executionTime =
                builder.AddParameter(
                    Envelope.ExecutionTime,
                    NpgsqlDbType.Timestamp);

            var body = builder.AddParameter(bytes, NpgsqlDbType.Bytea);

            var sql = $"insert into {_incomingTable} (id, owner_id, status, execution_time, body) values ({id.ParameterName}, {owner.ParameterName}, {status.ParameterName}, {executionTime.ParameterName})";
            builder.Append(sql);
        }

        public Type DocumentType => typeof(Envelope);
    }

    public class StoreOutgoingEnvelope : IStorageOperation
    {
        public Envelope Envelope { get; }
        private readonly DbObjectName _outgoingTable;
        private readonly int _ownerId;

        public StoreOutgoingEnvelope(DbObjectName outgoingTable, Envelope envelope, int ownerId)
        {
            // TODO -- do some assertions here

            Envelope = envelope;
            _outgoingTable = outgoingTable;
            _ownerId = ownerId;
        }

        public void ConfigureCommand(CommandBuilder builder)
        {
            Envelope.EnsureData();
            var bytes = Envelope.Serialize();

            var id = builder.AddParameter(Envelope.Id, NpgsqlDbType.Uuid);
            var owner = builder.AddParameter(_ownerId, NpgsqlDbType.Integer);
            var destination = builder.AddParameter(Envelope.Destination.ToString(), NpgsqlDbType.Varchar);
            var deliverBy =
                builder.AddParameter(
                    Envelope.DeliverBy,
                    NpgsqlDbType.Timestamp);

            var body = builder.AddParameter(bytes, NpgsqlDbType.Bytea);

            var sql = $"insert into {_outgoingTable} (id, owner_id, destination, deliver_by, body) values ({id.ParameterName}, {owner.ParameterName}, {destination.ParameterName}, {deliverBy.ParameterName})";
            builder.Append(sql);
        }

        public Type DocumentType => typeof(Envelope);
    }

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

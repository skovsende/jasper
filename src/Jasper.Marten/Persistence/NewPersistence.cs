using System;
using System.Collections.Generic;
using Jasper.Bus.Runtime;
using Marten;
using Marten.Schema;
using Marten.Services;
using Marten.Storage;
using Marten.Util;
using NpgsqlTypes;

namespace Jasper.Marten.Persistence
{
    public class PostgresqlEnvelopeStorage : FeatureSchemaBase
    {
        public const string TableName = "mt_envelopes";

        public PostgresqlEnvelopeStorage(StoreOptions options) : base("envelope-storage", options)
        {
        }

        protected override IEnumerable<ISchemaObject> schemaObjects()
        {
            yield return new EnvelopeTable(Options);
        }

        public override Type StorageType => typeof(Envelope);
    }

    public class EnvelopeTable : Table
    {
        public EnvelopeTable(StoreOptions options) : base(new DbObjectName(options.DatabaseSchemaName, PostgresqlEnvelopeStorage.TableName))
        {
            AddPrimaryKey(new TableColumn("id", "uuid"));
            AddColumn("status", "varchar", "NOT NULL");
            AddColumn("owner_id", "int", "NOT NULL");
            AddColumn("destination", "varchar");
            AddColumn("execution_time", "timestamp");
            AddColumn("body", "bytea", "NOT NULL");
        }
    }

    public class StoreEnvelope : IStorageOperation
    {
        public Envelope Envelope { get; }
        private readonly DbObjectName _envelopeTable;
        private readonly string _status;
        private readonly int _ownerId;

        public StoreEnvelope(DbObjectName envelopeTable, Envelope envelope, string status, int ownerId)
        {
            // TODO -- do some assertions here

            Envelope = envelope;
            _envelopeTable = envelopeTable;
            _status = status;
            _ownerId = ownerId;
        }

        public void ConfigureCommand(CommandBuilder builder)
        {
            Envelope.EnsureData();
            var bytes = Envelope.Serialize();

            var id = builder.AddParameter(Envelope.Id, NpgsqlDbType.Uuid);
            var status = builder.AddParameter(_status, NpgsqlDbType.Varchar);
            var owner = builder.AddParameter(_ownerId, NpgsqlDbType.Integer);
            var destination = builder.AddParameter(Envelope.Destination.ToString(), NpgsqlDbType.Varchar);
            var executionTime =
                builder.AddParameter(
                    Envelope.ExecutionTime,
                    NpgsqlDbType.Timestamp);

            var body = builder.AddParameter(bytes, NpgsqlDbType.Bytea);

            var sql = $"insert into {_envelopeTable} (id, status, owner_id, destination, execution_time, body) values ({id.ParameterName}, {status.ParameterName}, {owner.ParameterName}, {destination.ParameterName}, {executionTime.ParameterName})";
            builder.Append(sql);
        }

        public Type DocumentType => typeof(Envelope);
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Jasper.Bus;
using Jasper.Bus.Logging;
using Jasper.Bus.Runtime;
using Jasper.Bus.Transports;
using Jasper.Bus.Transports.Configuration;
using Jasper.Util;
using Marten;
using Marten.Util;
using NpgsqlTypes;

namespace Jasper.Marten.Persistence.Resiliency
{
    public class RecoverOutgoingMessages : IMessagingAction
    {
        public static readonly int OutgoingMessageLockId = "recover-incoming-messages".GetHashCode();
        private readonly IChannelGraph _channels;
        private readonly OwnershipMarker _marker;
        private readonly ISchedulingAgent _schedulingAgent;
        private readonly CompositeTransportLogger _logger;
        private readonly BusSettings _settings;
        private readonly string _findUniqueDestinations;
        private readonly string _findOutgoingEnvelopesSql;
        private readonly string _deleteOutgoingSql;

        public RecoverOutgoingMessages(IChannelGraph channels, BusSettings settings, OwnershipMarker marker, ISchedulingAgent schedulingAgent, CompositeTransportLogger logger)
        {
            _channels = channels;
            _settings = settings;
            _marker = marker;
            _schedulingAgent = schedulingAgent;
            _logger = logger;

            _findUniqueDestinations = $"select distinct destination from {_marker.Outgoing}";
            _findOutgoingEnvelopesSql = $"select body from {marker.Outgoing} where owner_id = {TransportConstants.AnyNode} and destination = :destination take {settings.Retries.RecoveryBatchSize}";
            _deleteOutgoingSql = $"delete from {marker.Outgoing} where owner_id = :owner and destination = :destination";


        }

        public async Task<List<Uri>> FindAllOutgoingDestinations(IDocumentSession session)
        {
            var list = new List<Uri>();

            var cmd = session.Connection.CreateCommand(_findUniqueDestinations);
            using (var reader = await cmd.ExecuteReaderAsync())
            {
                var text = await reader.GetFieldValueAsync<string>(0);
                list.Add(text.ToUri());
            }

            return list;
        }

        public async Task Execute(IDocumentSession session)
        {
            if (!await session.TryGetGlobalTxLock(OutgoingMessageLockId))
                return;


            var destinations = await FindAllOutgoingDestinations(session);

            var count = 0;
            foreach (var destination in destinations)
            {
                count += await recoverFrom(destination, session);
            }

            var wasMaxedOut = count >= _settings.Retries.RecoveryBatchSize;

            if (wasMaxedOut)
            {
                _schedulingAgent.RescheduleOutgoingRecovery();
            }
        }

        private async Task<int> recoverFrom(Uri destination, IDocumentSession session)
        {
            try
            {
                var channel = _channels.GetOrBuildChannel(destination);

                if (channel.Latched) return 0;

                var outgoing = await session.Connection.CreateCommand(_findOutgoingEnvelopesSql)
                    .With("destination", destination.ToString(), NpgsqlDbType.Varchar)
                    .ExecuteToEnvelopes();

                var filtered = filterExpired(session, outgoing);

                // Might easily try to do this in the time between starting
                // and having the data fetched. Was able to make that happen in
                // (contrived) testing
                if (channel.Latched || !filtered.Any()) return 0;

                await _marker.MarkOutgoingOwnedByThisNode(session, filtered);

                await session.SaveChangesAsync();

                _logger.RecoveredOutgoing(filtered);

                // TODO -- will need a compensating action here if any of this fails
                foreach (var envelope in filtered)
                {
                    await channel.QuickSend(envelope);
                }

                return outgoing.Count();

            }
            catch (UnknownTransportException e)
            {
                _logger.LogException(e, message: $"Could not resolve a channel for {destination}");

                await DeleteFromOutgoingEnvelopes(session, TransportConstants.AnyNode, destination);
                await session.SaveChangesAsync();

                return 0;
            }
        }

        public Task DeleteFromOutgoingEnvelopes(IDocumentSession session, int ownerId, Uri destination)
        {
            return session.Connection.CreateCommand(_deleteOutgoingSql)
                .With("destination", destination.ToString(), NpgsqlDbType.Varchar)
                .With("owner", ownerId, NpgsqlDbType.Integer).ExecuteNonQueryAsync();
        }


        private Envelope[] filterExpired(IDocumentSession session, IEnumerable<Envelope> outgoing)
        {
            var expiredMessages = outgoing.Where(x => x.IsExpired()).ToArray();
            _logger.DiscardedExpired(expiredMessages);

            foreach (var expired in expiredMessages)
            {
                session.Delete(expired);
            }

            return outgoing.Where(x => !x.IsExpired()).ToArray();
        }
    }
}

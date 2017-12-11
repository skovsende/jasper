using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Jasper.Bus.Logging;
using Jasper.Bus.Runtime;
using Jasper.Bus.Transports;
using Jasper.Bus.WorkerQueues;
using Marten;
using Marten.Linq;
using Marten.Util;
using NpgsqlTypes;

namespace Jasper.Marten.Persistence.Resiliency
{
    public class RunScheduledJobs : IMessagingAction
    {
        private readonly string _findReadyToExecuteJobs;
        private readonly IWorkerQueue _workers;
        private readonly IDocumentStore _store;
        private readonly OwnershipMarker _marker;
        private readonly CompositeTransportLogger _logger;
        public static readonly int ScheduledJobLockId = "scheduled-jobs".GetHashCode();

        public RunScheduledJobs(IWorkerQueue workers, IDocumentStore store, OwnershipMarker marker, CompositeTransportLogger logger)
        {
            _workers = workers;
            _store = store;
            _marker = marker;
            _logger = logger;

            _findReadyToExecuteJobs = $"select body from {marker.Incoming} where status = '{TransportConstants.Scheduled}' and execution_time >= :time";

        }

        public async Task Execute(IDocumentSession session)
        {
            var utcNow = DateTime.UtcNow;

            await ExecuteAtTime(session, utcNow);
        }

        public async Task<List<Envelope>> ExecuteAtTime(IDocumentSession session, DateTime utcNow)
        {
            if (!await session.TryGetGlobalTxLock(ScheduledJobLockId))
            {
                return null;
            }

            var readyToExecute = await session.Connection
                .CreateCommand(_findReadyToExecuteJobs)
                .With("time", DateTime.UtcNow, NpgsqlDbType.Timestamp)
                .ExecuteToEnvelopes();

            if (!readyToExecute.Any()) return readyToExecute;


            await _marker.MarkIncomingOwnedByThisNode(session, readyToExecute);

            await session.SaveChangesAsync();

            _logger.ScheduledJobsQueuedForExecution(readyToExecute);

            foreach (var envelope in readyToExecute)
            {
                envelope.Callback = new MartenCallback(envelope, _workers, _store, _marker);

                await _workers.Enqueue(envelope);
            }

            return readyToExecute;
        }
    }


}

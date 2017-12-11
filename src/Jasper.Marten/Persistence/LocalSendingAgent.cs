using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Jasper.Bus.Runtime;
using Jasper.Bus.Runtime.Serializers;
using Jasper.Bus.Transports.Sending;
using Jasper.Bus.WorkerQueues;
using Jasper.Conneg;
using Jasper.Marten.Persistence.Resiliency;
using Marten;

namespace Jasper.Marten.Persistence
{
    public class LocalSendingAgent : ISendingAgent
    {
        private readonly IWorkerQueue _queues;
        private readonly IDocumentStore _store;
        private readonly OwnershipMarker _marker;
        private readonly SerializationGraph _serializers;
        public Uri Destination { get; }

        public LocalSendingAgent(Uri destination, IWorkerQueue queues, IDocumentStore store, OwnershipMarker marker, SerializationGraph serializers)
        {
            _queues = queues;
            _store = store;
            _marker = marker;
            _serializers = serializers;
            Destination = destination;
        }

        public void Dispose()
        {
            // nothing
        }

        public Uri DefaultReplyUri { get; set; }

        public bool Latched => false;

        public Task EnqueueOutgoing(Envelope envelope)
        {
            envelope.Callback = new MartenCallback(envelope, _queues, _store, _marker);

            return _queues.Enqueue(envelope);
        }

        public async Task StoreAndForward(Envelope envelope)
        {
            using (var session = _store.LightweightSession())
            {
                writeMessageData(envelope);
                session.StoreIncoming(_marker, envelope);
                await session.SaveChangesAsync();
            }

            await EnqueueOutgoing(envelope);
        }

        public async Task StoreAndForwardMany(IEnumerable<Envelope> envelopes)
        {
            using (var session = _store.LightweightSession())
            {
                foreach (var envelope in envelopes)
                {
                    writeMessageData(envelope);

                    session.StoreIncoming(_marker, envelope);
                }



                await session.SaveChangesAsync();

                foreach (var envelope in envelopes)
                {
                    await EnqueueOutgoing(envelope);
                }
            }
        }

        private void writeMessageData(Envelope envelope)
        {
            if (envelope.Data == null || envelope.Data.Length == 0)
            {
                var writer = _serializers.JsonWriterFor(envelope.Message.GetType());
                envelope.Data = writer.Write(envelope.Message);
                envelope.ContentType = writer.ContentType;
            }
        }

        public void Start()
        {
            // Nothing
        }
    }
}

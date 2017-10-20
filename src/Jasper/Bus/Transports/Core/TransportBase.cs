using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Baseline;
using Baseline.Dates;
using Jasper.Bus.Logging;
using Jasper.Bus.Runtime;
using Jasper.Bus.Runtime.Invocation;
using Jasper.Bus.Transports.Configuration;
using Jasper.Util;

namespace Jasper.Bus.Transports.Core
{
    public abstract class TransportBase: ITransport, ISenderCallback
    {
        public string Protocol { get; }
        public CompositeLogger Logger { get; }
        public IPersistence Persistence { get; }

        private readonly TransportSettings _settings;

        private readonly CancellationTokenSource _cancellation = new CancellationTokenSource();
        private readonly SendingAgent _sender;
        private Uri _replyUri;
        private ListeningAgent _listener;

        protected TransportBase(string protocol, IPersistence persistence, CompositeLogger logger, ISenderProtocol sendingProtocol, BusSettings settings)
        {
            _sender = new SendingAgent(sendingProtocol);
            Protocol = protocol;
            Persistence = persistence;
            Logger = logger;
            _settings = settings[Protocol];
        }

        public void Dispose()
        {
            _cancellation.Cancel();

            _sender?.Dispose();

            _listener?.Dispose();
            //TODO: wait for the receivers to stop.
        }


        private void processRetry(OutgoingMessageBatch outgoing)
        {
            foreach (var message in outgoing.Messages)
            {
                message.SentAttempts++;
            }

            var groups = outgoing
                .Messages
                .Where(x => x.SentAttempts < _settings.MaximumSendAttempts)
                .GroupBy(x => x.SentAttempts);

            foreach (var group in groups)
            {
                var delayTime = (group.Key * group.Key).Seconds();
                var messages = group.ToArray();

                Task.Delay(delayTime, _cancellation.Token).ContinueWith(_ =>
                {
                    if (_cancellation.IsCancellationRequested)
                    {
                        return;
                    }

                    foreach (var message in messages)
                    {
                        _sender.Enqueue(message);
                    }
                });
            }

            Persistence.PersistBasedOnSentAttempts(outgoing, _settings.MaximumSendAttempts);
        }

        void ISenderCallback.Successful(OutgoingMessageBatch outgoing)
        {
            Persistence.RemoveOutgoing(outgoing.Messages);
        }

        void ISenderCallback.TimedOut(OutgoingMessageBatch outgoing)
        {
            processRetry(outgoing);
        }

        void ISenderCallback.SerializationFailure(OutgoingMessageBatch outgoing)
        {
            processRetry(outgoing);
        }

        void ISenderCallback.QueueDoesNotExist(OutgoingMessageBatch outgoing)
        {
            processRetry(outgoing);
        }

        void ISenderCallback.ProcessingFailure(OutgoingMessageBatch outgoing)
        {
            processRetry(outgoing);
        }

        void ISenderCallback.ProcessingFailure(OutgoingMessageBatch outgoing, Exception exception)
        {
            processRetry(outgoing);
        }

        public Task Send(Envelope envelope, Uri destination)
        {
            if ((envelope.ReplyRequested.IsNotEmpty() || envelope.AckRequested) && _listener == null)
            {
                throw new InvalidOperationException($"Transport '{Protocol}' cannot send messages with reply-requested or ack-requested without a receiver for replies");
            }

            if (envelope.Data == null)
            {
                envelope.WriteData();
            }

            envelope.Destination = destination;
            enqueueOutgoing(envelope);

            return Task.CompletedTask;
        }

        private void enqueueOutgoing(Envelope envelope)
        {
            envelope.ReplyUri = _replyUri;

            Persistence.StoreOutgoing(envelope);

            _sender.Enqueue(envelope);
        }

        public Uri DefaultReplyUri()
        {
            return _replyUri;
        }


        public TransportState State => _settings.State;
        public void Describe(TextWriter writer)
        {
            if (_settings?.Port != null)
            {
                writer.WriteLine($"Listening for messages at {_settings.Port} with protocol '{Protocol}'");
            }
        }


        public IChannel[] Start(IHandlerPipeline pipeline, BusSettings settings)
        {
            if (_settings.State == TransportState.Disabled) return new IChannel[0];

            startHandlingIncoming(pipeline, settings);

            return startHandlingOutgoing(settings);
        }

        private IChannel[] startHandlingOutgoing(BusSettings settings)
        {
            _sender.Start(this);

            Persistence.RecoverOutgoingMessages(enqueueOutgoing, _cancellation.Token);

            return settings.KnownSubscribers.Where(x => x.Uri.Scheme == Protocol)
                .Select(x => new Channel(x, this)).OfType<IChannel>().ToArray();
        }

        private void startHandlingIncoming(IHandlerPipeline pipeline, BusSettings settings)
        {
            var queueNames = _settings.AllQueueNames();
            Persistence.Initialize(queueNames);

            var queues = new TcpQueues(Persistence, Logger, _cancellation.Token);

            // make sure the default queue is included
            var queuesToHandle = _settings.Concat(new[]
            {
                new QueueSettings(TransportConstants.Default) {Parallelization = 5}
            });
            foreach (var queue in queuesToHandle)
            {
                queues.EnsureQueue(queue.Name, queueReader =>
                {
                    var receiver = new QueueReceiver(queue.Name, queue.Parallelization, pipeline, queueReader, _cancellation.Token);
                    receiver.Start();
                    //TODO: hold on to the receivers so we can wait for them to complete when we're done.
                });
            }

            queues.RecoverPersistedMessages(_cancellation.Token);

            if (_settings.Port.HasValue)
            {
                _replyUri = $"{Protocol}://{settings.MachineName}:{_settings.Port}/{TransportConstants.Replies}"
                    .ToUri();


                _listener = new ListeningAgent(queues, _settings.Port.Value, Protocol, _cancellation.Token);
                _listener.Start();
            }
        }

    }
}

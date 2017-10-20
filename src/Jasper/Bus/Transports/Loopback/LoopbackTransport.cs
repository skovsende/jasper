using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Jasper.Bus.Logging;
using Jasper.Bus.Runtime;
using Jasper.Bus.Runtime.Invocation;
using Jasper.Bus.Transports.Configuration;
using Jasper.Bus.Transports.Core;
using Jasper.Bus.Transports.Lightweight;
using Jasper.Util;

namespace Jasper.Bus.Transports.Loopback
{
    public class LoopbackTransport : ITransport
    {
        public static readonly string ProtocolName = "loopback";

        private readonly CancellationTokenSource _cancellation = new CancellationTokenSource();
        private readonly CompositeLogger _logger;

        private readonly Dictionary<string, LoopbackQueue> _processors = new Dictionary<string, LoopbackQueue>();

        public LoopbackTransport(CompositeLogger logger)
        {
            _logger = logger;
        }

        public string Protocol => ProtocolName;

        public TransportState State { get; } = TransportState.Enabled;
        public void Describe(TextWriter writer)
        {
            if (_settings == null) return;
            foreach (var setting in _settings)
            {
                writer.WriteLine($"Processing messages at loopback queue '{setting.Name}'");
            }
        }

        public Task Send(Envelope envelope, Uri destination)
        {
            var processor = _processors[destination.QueueName()];
            return processor.Enqueue(envelope);
        }

        public IChannel[] Start(IHandlerPipeline pipeline, BusSettings settings)
        {
            _settings = settings.Loopback;
            return startListeners(pipeline, settings).ToArray();
        }

        private IEnumerable<IChannel> startListeners(IHandlerPipeline pipeline, BusSettings settings)
        {
            var senders = settings.KnownSubscribers.Where(x => x.Uri.Scheme == ProtocolName)
                .ToDictionary(x => x.Uri);

            foreach (var queue in settings.Loopback)
            {
                if (!_processors.ContainsKey(queue.Name))
                {
                    _processors.Add(queue.Name, makeProcessor(queue, pipeline));
                }
                var uri = $"loopback://{queue.Name}".ToUri();

                senders.TryGetValue(uri, out SubscriberAddress subscriber);
                yield return new LoopbackChannel(subscriber ?? new SubscriberAddress(uri), this);
            }

            foreach (var sender in senders.Values)
            {
                var queueName = sender.Uri.QueueName();
                if (!_processors.ContainsKey(queueName))
                {
                    var queueSettings = new QueueSettings(queueName) { Parallelization = 5};
                    _processors.Add(queueName, makeProcessor(queueSettings, pipeline));
                }
                yield return new LoopbackChannel(sender, this);
            }
        }

        private LoopbackQueue makeProcessor(QueueSettings queueSettings, IHandlerPipeline pipeline)
        {
            return new LoopbackQueue(queueSettings.Name, queueSettings.Parallelization, pipeline, _cancellation.Token);
        }

        public Uri DefaultReplyUri()
        {
            return TransportConstants.RetryUri;
        }

        public bool Enabled { get; } = true;

        public void Dispose()
        {
            _cancellation.Cancel();
            foreach (var block in _processors.Values)
            {
                block.Dispose();
            }
        }

        public static readonly Uri Delayed = "loopback://delayed".ToUri();
        public static readonly Uri Retries = "loopback://retries".ToUri();
        private TransportSettings _settings;

    }

    public class LoopbackQueue : IQueueReader, IDisposable
    {
        private readonly BufferBlock<Envelope> _buffer = new BufferBlock<Envelope>();
        private readonly QueueReceiver _receiver;

        public LoopbackQueue(string queueName, int maximumParallelization, IHandlerPipeline pipeline, CancellationToken cancellationToken)
        {
            _receiver = new QueueReceiver(queueName, maximumParallelization, pipeline, this, cancellationToken);
            _receiver.Start();
        }

        public Task<Envelope> PeekLock(CancellationToken cancellationToken)
        {
            return _buffer.ReceiveAsync(cancellationToken);
        }

        public Task Receive(Envelope envelope, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public void Dispose()
        {
            _receiver.Dispose();
        }

        public Task Enqueue(Envelope envelope)
        {
            throw new NotImplementedException("Need to figure out how to build the proper callback for the loopback transport");
            envelope.Callback = new LightweightCallback(null);
            return _buffer.SendAsync(envelope);
        }
    }
}

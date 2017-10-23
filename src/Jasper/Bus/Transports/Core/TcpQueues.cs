using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Jasper.Bus.Logging;
using Jasper.Bus.Runtime;

namespace Jasper.Bus.Transports.Core
{
    public class TcpQueues : ITcpReceiverCallback
    {
        private readonly IPersistence _persistence;
        private readonly IBusLogger _logger;
        private readonly CancellationToken _cancellationToken;
        private readonly Dictionary<string,TcpQueue> _queues = new Dictionary<string, TcpQueue>();

        public TcpQueues(IPersistence persistence, IBusLogger logger, CancellationToken cancellationToken)
        {
            _persistence = persistence;
            _logger = logger;
            _cancellationToken = cancellationToken;
        }

        public void EnsureQueue(string queueName, Action<IQueueReader> onCreated)
        {
            if (!_queues.ContainsKey(queueName))
            {
                var inMemoryQueue = new TcpQueue(_persistence, messageCallbackCreator);
                _queues.Add(queueName, inMemoryQueue);
                onCreated.Invoke(inMemoryQueue);
            }
        }

        private IMessageCallback messageCallbackCreator(TcpQueue queue, Envelope envelope)
        {
            return new TcpQueueMessageCallback("", envelope, queue, _cancellationToken);
        }

        public void RecoverPersistedMessages(CancellationToken cancellationToken)
        {
            Task.Run(async () =>
            {
                IEnumerable<Envelope> inboxMessages = await _persistence.LoadInbox(_queues.Keys.ToArray(), cancellationToken);
                foreach (var inboxMessage in inboxMessages)
                {
                    enqueue(inboxMessage);
                }
            }, cancellationToken).Wait(cancellationToken);
        }

        public async Task<ReceivedStatus> Received(Uri uri, Envelope[] messages)
        {
            try
            {
                //HACK: until we make IPersistence.StoreInitial async
                await Task.CompletedTask;

                _persistence.StoreInitial(messages);

                foreach (var message in messages)
                {
                    message.ReceivedAt = uri;
                    enqueue(message);
                }

                return ReceivedStatus.Successful;
            }
            catch (Exception e)
            {
                _logger.LogException(e);
                return ReceivedStatus.ProcessFailure;
            }
        }

        private void enqueue(Envelope message)
        {
            if (_queues.ContainsKey(message.Queue))
            {
                _queues[message.Queue].Post(message);
            }
            else
            {
                _queues[TransportConstants.Default].Post(message);
            }
        }

        public void Acknowledged(Envelope[] messages)
        {
            //Question: should this be the point where the messages are actually made available to be processed? (i.e. it appears the be the second phase of two-phase commit)
            // but then we have to deal with in-doubt transactions if neither the acknowledged nor notacknowledged arrives.

            // for now, we'll do nothing.
        }

        public void NotAcknowledged(Envelope[] messages)
        {
            // apparently we need to pull the messages back out
            //TODO: how do we prevent them from being processed or cause them to fail when they attempt to "Receive"?
            _persistence.Remove(messages);
        }

        public void Failed(Exception exception, Envelope[] messages)
        {
            _logger.LogException(new MessageFailureException(messages, exception));
        }
    }

    public class TcpQueue : IQueueReader
    {
        private readonly IPersistence _persistence;
        private readonly Func<TcpQueue, Envelope, IMessageCallback> _messageCallbackCreator;
        private readonly BufferBlock<Envelope> _buffer = new BufferBlock<Envelope>();

        public TcpQueue(IPersistence persistence, Func<TcpQueue,Envelope, IMessageCallback> messageCallbackCreator)
        {
            _persistence = persistence;
            _messageCallbackCreator = messageCallbackCreator;
        }
        public async Task<Envelope> PeekLock(CancellationToken cancellationToken)
        {
            Envelope candidateEnvelope;
            // keep going through the items in the buffer until we find one we can claim
            do
            {
                //TODO: put a timeout on the ReceiveAsync and if we time out, go looking in the DB for any messages that might have been left unhandled by another node
                candidateEnvelope = await _buffer.ReceiveAsync(cancellationToken);
                //TODO: start the message handling scope and use that scope for a database connection and transaction and to hold the lock
            } while (!await _persistence.TryClaim(candidateEnvelope));

            return candidateEnvelope;
        }

        public Task Receive(Envelope envelope, CancellationToken cancellationToken)
        {
            _persistence.Remove(envelope.Queue, envelope);
            return Task.CompletedTask;
        }

        public void Post(Envelope envelope)
        {
            envelope.Callback = _messageCallbackCreator.Invoke(this, envelope);
            _buffer.Post(envelope);
        }

        public Task MoveToError(Envelope envelope, ErrorReport report)
        {
            // There's an outstanding issue for actually doing error reports
            return Task.CompletedTask;
        }

        public Task Requeue(Envelope originalEnvelope, Envelope newEnvelope)
        {
            return Task.Run(() =>
            {
                _persistence.Replace(originalEnvelope.Queue, newEnvelope);
                _buffer.Post(newEnvelope);
            });
        }
    }
}

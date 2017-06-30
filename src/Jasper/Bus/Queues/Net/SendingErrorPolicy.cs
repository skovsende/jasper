﻿using System;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using Jasper.Bus.Queues.Logging;
using Jasper.Bus.Queues.Storage;
using Jasper.Bus.Runtime;

namespace Jasper.Bus.Queues.Net
{
    public class SendingErrorPolicy
    {
        private readonly ILogger _logger;
        private readonly IMessageStore _store;
        private readonly IScheduler _scheduler;

        public SendingErrorPolicy(ILogger logger, IMessageStore store,
            IObservable<OutgoingMessageFailure> failedToConnect, IScheduler scheduler)
        {
            _logger = logger;
            _store = store;
            _scheduler = scheduler;
            RetryStream = failedToConnect.SelectMany(x => x.Batch.Messages)
                .Do(IncrementAttempt)
                .Where(ShouldRetry)
                .SelectMany(x => Observable.Return(x)
                    .Delay(TimeSpan.FromSeconds(x.SentAttempts * x.SentAttempts), _scheduler))
                .Finally(() => _logger.Debug("SendingErrorPolicy stream ended"));
        }

        public SendingErrorPolicy(ILogger logger, IMessageStore store, IObservable<OutgoingMessageFailure> failedToConnect)
            : this(logger, store, failedToConnect, new EventLoopScheduler())
        {

        }

        public IObservable<Envelope> RetryStream { get; }

        public bool ShouldRetry(Envelope message)
        {
            var totalAttempts = message.MaxAttempts ?? 100;
            _logger.DebugFormat("Failed to send should retry with AttemptCount: {0}, TotalAttempts {1}", message.SentAttempts, totalAttempts);
            if(message.DeliverBy.HasValue)
                _logger.DebugFormat("Failed to send should retry with DeliverBy: {0}, CurrentTime {1}", message.DeliverBy, DateTime.Now);
            return (message.SentAttempts < totalAttempts)
                &&
                (!message.DeliverBy.HasValue || DateTime.Now < message.DeliverBy);
        }

        private void IncrementAttempt(Envelope message)
        {
            try
            {
                message.SentAttempts++;
                _store.FailedToSend(message);
            }
            catch (Exception ex)
            {
                _logger.Error("Failed to increment send failure", ex);
            }
        }
    }
}

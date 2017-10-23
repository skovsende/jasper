using System;
using System.Threading;
using System.Threading.Tasks;
using Jasper.Bus.Runtime.Invocation;

namespace Jasper.Bus.Transports.Core
{
    public class QueueReceiver : IDisposable
    {
        private readonly IHandlerPipeline _pipeline;
        private readonly int _maximumParallelization;
        private readonly IQueueReader _queue;
        private readonly CancellationToken _cancellationToken;
        private Task _receivingTask;
        private readonly SemaphoreSlim _parallelHandlingCapacity;

        public QueueReceiver(string queueName, int maximumParallelization, IHandlerPipeline pipeline, IQueueReader queueReader, CancellationToken cancellationToken)
        {
            _pipeline = pipeline;
            _maximumParallelization = maximumParallelization;
            _queue = queueReader;
            _cancellationToken = cancellationToken;
            QueueName = queueName;
            _parallelHandlingCapacity = new SemaphoreSlim(maximumParallelization);
        }

        public string QueueName { get; }

        public void Start()
        {
            //TODO: probably this Start method should be made idempotent.
            _receivingTask = receiveMessages();
        }

        public void Dispose()
        {
            // We want the following to actually wait even after cancellation
            // has been requested, so we're not passing the cancellation
            // token to it.
            // ReSharper disable once MethodSupportsCancellation
            _receivingTask.Wait();
        }

        private async Task receiveMessages()
        {
            // this will cause at most _maximumParallelization simultaneous tasks to be waiting for the PeekLock task to complete.
            while (!_cancellationToken.IsCancellationRequested)
            {
                await _parallelHandlingCapacity.WaitAsync(_cancellationToken).ConfigureAwait(false);

#pragma warning disable 4014
                Task.Run(
                        async () =>
                        {
                            var envelope = await _queue.PeekLock(_cancellationToken).ConfigureAwait(false);

                            envelope.ContentType = envelope.ContentType ?? "application/json";
                            await _pipeline.Invoke(envelope).ConfigureAwait(false);
                        },
                        _cancellationToken
                    )
                    .ContinueWith(
                        (tsk, o) =>
                        {
                            _parallelHandlingCapacity.Release();
                            //TODO: observe and report faults. We can ignore cancellation.
                        },
                        state: null,
                        continuationOptions: TaskContinuationOptions.ExecuteSynchronously
                    );
#pragma warning restore 4014
            }

            // now wait until all the handler tasks complete
            for (int i = 0; i < _maximumParallelization; i++)
            {
                // We know cancellation has already been requested, now we're
                // waiting for it to be acknowledged; we don't pass the
                // cancellation token here because we want it to actually wait
                // instead of immediately throwing OperationCancelledException.
                // ReSharper disable once MethodSupportsCancellation
                await _parallelHandlingCapacity.WaitAsync().ConfigureAwait(false);
            }
        }
    }
}

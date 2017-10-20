using System;
using System.Threading;
using System.Threading.Tasks;
using Jasper.Bus.Runtime;
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

        public string QueueName { get; }

        public QueueReceiver(string queueName, int maximumParallelization, IHandlerPipeline pipeline, IQueueReader queueReader, CancellationToken cancellationToken)
        {
            _pipeline = pipeline;
            _maximumParallelization = maximumParallelization;
            _queue = queueReader;
            _cancellationToken = cancellationToken;
            QueueName = queueName;
        }

        private async Task receiveMessages()
        {
            var sem = new SemaphoreSlim(_maximumParallelization);
            while (!_cancellationToken.IsCancellationRequested)
            {
                await sem.WaitAsync(_cancellationToken);

#pragma warning disable 4014
                Task.Run(async () =>
#pragma warning restore 4014
                {
                    Envelope envelope;
                    try
                    {
                        envelope = await _queue.PeekLock(_cancellationToken);
                    }
                    catch (OperationCanceledException)
                    {
                        sem.Release();
                        return;
                    }

                    await receive(envelope)
                        //TODO: capture and report failure
                        .ContinueWith((tsk, o) =>
                        {
                            sem.Release();
                        }, null, TaskContinuationOptions.ExecuteSynchronously);
                }, _cancellationToken);
                //TODO: append a continuation that observes and reports faults including cancellation.
            }
            // now wait until the pending handler tasks complete
            for (int i = 0; i < _maximumParallelization; i++)
            {
                // ReSharper disable once MethodSupportsCancellation
                // We know cancellation has already been requested, now we're jwaiting for it to be acknowledged; we don't want these to fail
                await sem.WaitAsync();
            }
        }

        private Task receive(Envelope envelope)
        {
            envelope.ContentType = envelope.ContentType ?? "application/json";

            return _pipeline.Invoke(envelope);
        }

        public void Dispose()
        {
            _receivingTask.Wait();
        }

        public void Start()
        {
            _receivingTask = receiveMessages();
        }
    }


}

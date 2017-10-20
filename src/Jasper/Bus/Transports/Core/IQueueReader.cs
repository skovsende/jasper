using System.Threading;
using System.Threading.Tasks;
using Jasper.Bus.Runtime;

namespace Jasper.Bus.Transports.Core
{
    public interface IQueueReader
    {
        Task<Envelope> PeekLock(CancellationToken cancellationToken);
        Task Receive(Envelope envelope, CancellationToken cancellationToken);
    }
}

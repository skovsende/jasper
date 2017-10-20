using System.Threading;
using System.Threading.Tasks;
using Jasper.Bus.Runtime;

namespace Jasper.Bus.Transports.Core
{
    public interface IQueueProvider
    {
        // TODO -- make this async
        Task StoreIncomingMessages(Envelope[] messages);

        /// <summary>
        /// Removes messages that we have stored and reported received to the sender, but the sender didn't acknowledge.
        /// </summary>
        /// <param name="messages"></param>
        void RemoveIncomingMessages(Envelope[] messages);
    }
}

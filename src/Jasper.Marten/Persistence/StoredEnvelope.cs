using System;
using Jasper.Bus.Runtime;

namespace Jasper.Marten
{
    public class StoredEnvelope
    {
        public StoredEnvelope(Envelope wrapped, string status)
        {
            Envelope = wrapped;
            Status = status;
        }

        public string Id => Envelope.Id;

        public Envelope Envelope { get; private set; }

        public string Status { get; private set; }
    }
}

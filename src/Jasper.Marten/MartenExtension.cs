using System.Collections;
using Jasper;
using Jasper.Configuration;
using Jasper.Marten;
using Jasper.Marten.Codegen;
using Jasper.Marten.Outbox;
using Marten;
using Microsoft.Extensions.DependencyInjection;

// SAMPLE: MartenExtension
[assembly:JasperModule(typeof(MartenExtension))]

namespace Jasper.Marten
{
    public class MartenExtension : IJasperExtension
    {
        public void Configure(JasperRegistry registry)
        {
            registry.Settings.Require<StoreOptions>();

            registry.Services.AddSingleton<OutboxCommitListener>();

            registry.Services.AddSingleton<IDocumentStore>(x =>
            {
                var storeOptions = x.GetService<StoreOptions>();
                var documentStore = new DocumentStore(storeOptions);
                storeOptions.Listeners.Add(x.GetService<OutboxCommitListener>());
                return documentStore;
            });

            registry.Services.AddScoped(c => c.GetService<IDocumentStore>().OpenSession());
            registry.Services.AddScoped(c => c.GetService<IDocumentStore>().QuerySession());
            registry.Services.AddScoped<MartenOutbox>().For<IMartenOutbox>();

            registry.Generation.Sources.Add(new SessionVariableSource());

        }
    }
}
// ENDSAMPLE

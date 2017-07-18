﻿using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Consul;
using Jasper.Bus.Configuration;
using Jasper.Bus.Runtime;
using Jasper.Bus.Runtime.Subscriptions;

namespace Jasper.Consul.Internal
{
    public class ConsulNodeDiscovery : ConsulService, INodeDiscovery
    {
        public const string TRANSPORTNODE_PREFIX = GLOBAL_PREFIX + "node/";

        public ConsulNodeDiscovery(ConsulSettings settings, ChannelGraph channels, EnvironmentSettings envSettings) : base(settings, channels, envSettings)
        {
        }


        public Task Register(ChannelGraph graph)
        {
            LocalNode = new TransportNode(graph, MachineName);

            var consulKey = $"{TRANSPORTNODE_PREFIX}{LocalNode.NodeName}/{MachineName}";

            return client.KV.Put(new KVPair(TRANSPORTNODE_PREFIX + "/")
            {
                Value = serialize(LocalNode)
            });
        }

        public async Task<TransportNode[]> FindPeers()
        {
            var nodes = await client.KV.List(TRANSPORTNODE_PREFIX);
            return nodes.Response?.Select(kv => deserialize<TransportNode>(kv.Value)).ToArray() ?? new TransportNode[0];
        }

        public TransportNode LocalNode { get; set; }
    }
}